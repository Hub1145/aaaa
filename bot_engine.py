import json
import math
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
import time
import logging
import threading
import asyncio
from datetime import datetime
from collections import deque
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException
from translations_py import TRANSLATIONS

class BinanceTradingBotEngine:
    def __init__(self, config_path, emit_callback, server_ip="Unknown"):
        self.config_path = config_path
        self.emit = emit_callback
        self.server_ip = server_ip
        self.console_logs = deque(maxlen=500)
        self.config = self._load_config()
        self.language = self.config.get('language', 'pt-BR')
        self.bg_clients = {} # account_index -> { 'client': Client, 'name': str }
        self._initialize_bg_clients()
        self.metadata_client = self._get_metadata_client()
        self.market_client = self._create_client("", "")
        self.market_twm = None

        self.is_running = False
        self.stop_event = threading.Event()

        self.accounts = {} # account_index -> { 'client': Client, 'twm': ThreadedWebsocketManager, 'info': account_config }
        self.exchange_info = {} # symbol -> info

        # Shared market data: symbol -> { 'price': float, 'last_update': float, 'info': info }
        self.shared_market_data = {}
        self.market_data_lock = threading.Lock()
        self.max_leverages = {} # symbol -> max_leverage
        self.trailing_state = {} # (idx, symbol) -> { 'peak': float }

        # Grid state: (account_index, symbol) -> { 'initial_filled': bool, 'levels': { level: { 'tp_id': id, 'rb_id': id } } }
        self.grid_state = {}

        # Threads: (account_index, symbol) -> Thread
        self.symbol_threads = {}
        
        # Dashboard metrics
        self.account_balances = {} # account_index -> balance
        self.account_errors = {} # account_index -> error message
        self.account_last_update = {} # account_index -> timestamp
        self.open_positions = {} # account_index -> [positions]
        # Trailing TP/SL/Buy state
        self.trailing_state = {}
        self.last_log_times = {} # key -> timestamp
        
        self.data_lock = threading.Lock()
        
        self._setup_logging()
        
        # Start global background tasks immediately (pricing, metrics)
        self._background_tasks_started = False
        self._initialize_market_ws()
        threading.Thread(target=self._global_background_worker, daemon=True).start()

    def _setup_logging(self):
        numeric_level = logging.INFO
        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(ch)
        fh = logging.FileHandler('binance_bot.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(fh)

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            # Migration: If accounts don't have symbol_strategies, but global one exists, copy it
            global_strategies = config.get('symbol_strategies', {})
            api_accounts = config.get('api_accounts', [])
            modified = False
            for acc in api_accounts:
                if 'symbol_strategies' not in acc:
                    # Deep copy of global strategies to ensure account isolation
                    acc['symbol_strategies'] = {k: v.copy() for k, v in global_strategies.items()}
                    modified = True

            if modified:
                try:
                    with open(self.config_path, 'w') as f:
                        json.dump(config, f, indent=2)
                except: pass

            return config
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            return {}

    def _get_strategy(self, idx, symbol):
        """Helper to get strategy for a specific account and symbol."""
        api_accounts = self.config.get('api_accounts', [])
        if 0 <= idx < len(api_accounts):
            acc = api_accounts[idx]
            acc_strategies = acc.get('symbol_strategies', {})
            if symbol in acc_strategies:
                return acc_strategies[symbol]

        # Fallback to global symbol_strategies
        return self.config.get('symbol_strategies', {}).get(symbol, {})

    def _t(self, key, **kwargs):
        """Helper to get translated strings."""
        lang = self.language if self.language in TRANSLATIONS else 'pt-BR'
        template = TRANSLATIONS[lang].get(key, key)
        try:
            return template.format(**kwargs)
        except Exception:
            return template

    def log(self, message_or_key, level='info', account_name=None, is_key=False, **kwargs):
        # We store structured logs now to allow re-translation
        timestamp = datetime.now().strftime('%H:%M:%S')

        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'account_name': account_name,
            'key': message_or_key if is_key else None,
            'message': message_or_key if not is_key else None,
            'kwargs': kwargs
        }

        # Add a rendered version for the immediate display
        rendered_msg = self._render_log(log_entry)
        log_entry['rendered'] = rendered_msg

        self.console_logs.append(log_entry)
        self.emit('console_log', log_entry)

        if level == 'error': logging.error(rendered_msg)
        elif level == 'warning': logging.warning(rendered_msg)
        else: logging.info(rendered_msg)

    def _render_log(self, entry):
        account_name = entry.get('account_name')
        prefix = f"[{account_name}] " if account_name else ""

        if entry.get('key'):
            message = self._t(entry['key'], **entry.get('kwargs', {}))
        else:
            message = entry.get('message', '')

        return f"{prefix}{message}"

    def _create_client(self, api_key, api_secret):
        testnet = self.config.get('is_demo', True)
        # Enable automatic time synchronization and trim keys
        # We don't set API_URL manually to avoid breaking standard calls
        # Use a shorter timeout for the initial connection to avoid blocking too long
        client = Client(api_key.strip(), api_secret.strip(), testnet=testnet, requests_params={'timeout': 10})

        # Explicitly set Futures URL if we are in Demo mode to avoid any confusion with python-binance defaults
        if testnet:
            client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
            client.FUTURES_DATA_URL = 'https://testnet.binancefuture.com/fapi'
        else:
            client.FUTURES_URL = 'https://fapi.binance.com/fapi'
            client.FUTURES_DATA_URL = 'https://fapi.binance.com/fapi'

        try:
            # Use futures_time for futures accounts to avoid 404s on restricted spot regions
            res = client.futures_time()
            client.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
        except Exception as e:
            # Fallback to spot time if futures_time fails
            try:
                res = client.get_server_time()
                client.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
            except:
                logging.warning(f"Failed to sync time for account: {e}")
        return client

    def _get_client(self, api_key, api_secret):
        return self._create_client(api_key, api_secret)

    def _initialize_market_ws(self):
        """Initializes WebSocket for market data (prices)."""
        try:
            if self.market_twm:
                try: self.market_twm.stop()
                except: pass
                time.sleep(0.5)

            testnet = self.config.get('is_demo', True)
            self.market_twm = ThreadedWebsocketManager(testnet=testnet)
            self.market_twm.start()

            symbols = self.config.get('symbols', [])
            if symbols:
                for symbol in symbols:
                    try:
                        self.market_twm.start_futures_ticker_socket(
                            callback=self._handle_market_ticker,
                            symbol=symbol
                        )
                    except Exception as e:
                        logging.debug(f"Failed to start stream for {symbol}: {e}")
                logging.info(f"Started market WebSocket for {len(symbols)} symbols")
        except Exception as e:
            logging.error(f"Failed to initialize market WebSocket: {e}")

    def _handle_market_ticker(self, msg):
        """Callback for market ticker WebSocket."""
        try:
            # Handle both single ticker and array if needed
            data_list = msg if isinstance(msg, list) else [msg]

            for item in data_list:
                if item.get('e') == '24hrTicker':
                    symbol = item.get('s')
                    bid = float(item.get('b', 0))
                    ask = float(item.get('a', 0))
                    last = float(item.get('c', 0))

                    with self.market_data_lock:
                        if symbol not in self.shared_market_data:
                            self.shared_market_data[symbol] = {'price': 0, 'last_update': 0}

                        data = self.shared_market_data[symbol]
                        data['price'] = last
                        data['bid'] = bid
                        data['ask'] = ask
                        data['last_update'] = time.time()
        except Exception as e:
            pass

    def _initialize_bg_clients(self):
        """Initializes clients for all accounts with keys to fetch balances in background."""
        api_accounts = self.config.get('api_accounts', [])
        testnet = self.config.get('is_demo', True)

        # Cleanup existing background clients if they are no longer in the config, keys changed, or mode changed
        old_bg_idxs = list(self.bg_clients.keys())
        for idx in old_bg_idxs:
            if idx >= len(api_accounts):
                del self.bg_clients[idx]
                continue

            acc = api_accounts[idx]
            old_bg = self.bg_clients[idx]
            old_testnet = old_bg.get('is_demo', not testnet)

            if (acc.get('api_key', '').strip() != old_bg['info'].get('api_key', '').strip() or
                acc.get('api_secret', '').strip() != old_bg['info'].get('api_secret', '').strip() or
                old_testnet != testnet):
                del self.bg_clients[idx]

        new_bg_clients = self.bg_clients.copy()
        mode_str = "DEMO (Testnet)" if testnet else "LIVE (Mainnet)"

        for i, acc in enumerate(api_accounts):
            if i in new_bg_clients:
                continue # Already have a valid one

            api_key = acc.get('api_key', '').strip()
            api_secret = acc.get('api_secret', '').strip()
            # We initialize background clients for ALL accounts that have keys,
            # so we can show their balance even if they are not enabled for trading.
            if api_key and api_secret:
                try:
                    # Always re-create client to ensure correct environment (Demo vs Live)
                    client = self._get_client(api_key, api_secret)
                    new_bg_clients[i] = {
                        'client': client,
                        'name': acc.get('name', f"Account {i+1}"),
                        'info': acc,
                        'is_demo': testnet
                    }
                except Exception as e:
                    logging.error(f"Failed to init bg client for {acc.get('name')}: {e}")

        self.bg_clients = new_bg_clients
        logging.info(f"Initialized {len(new_bg_clients)} background clients in {mode_str} mode.")

    def _get_metadata_client(self):
        """Creates a client for fetching prices/leverage even when bot is stopped."""
        try:
            testnet = self.config.get('is_demo', True)
            accs = self.config.get('api_accounts', [])
            # Try to find an account with keys
            for acc in accs:
                if acc.get('api_key') and acc.get('api_secret'):
                    return self._create_client(acc['api_key'], acc['api_secret'])
            return self._create_client("", "")
        except:
            return None

    @staticmethod
    def test_account(api_key, api_secret, is_demo=True):
        try:
            client = Client(api_key.strip(), api_secret.strip(), testnet=is_demo, requests_params={'timeout': 20})
            client.futures_account_balance()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)

    def _init_account(self, i, acc):
        try:
            api_key = acc.get('api_key', '').strip()
            api_secret = acc.get('api_secret', '').strip()
            client = self._get_client(api_key, api_secret)
            twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=self.config.get('is_demo', True))
            twm.start()

            self.accounts[i] = {
                'client': client,
                'twm': twm,
                'info': acc,
                'last_update': 0
            }

            # Start user data stream
            twm.start_futures_user_socket(callback=lambda msg, idx=i: self._handle_user_data(idx, msg))

            # Initialize strategy for each symbol in its own thread
            for symbol in self.config.get('symbols', []):
                self._start_symbol_thread(i, symbol)

            self.log("account_init", account_name=acc.get('name'), is_key=True, name=acc.get('name', i))
        except Exception as e:
            self.log("account_init_failed", level='error', is_key=True, name=acc.get('name', i), error=str(e))

    def start(self):
        if self.is_running: return
        self.is_running = True
        self.stop_event.clear()

        self.log("bot_starting", is_key=True)

        # Initialize accounts
        api_accounts = self.config.get('api_accounts', [])
        for i, acc in enumerate(api_accounts):
            if acc.get('api_key') and acc.get('api_secret') and acc.get('enabled', True):
                self._init_account(i, acc)

    def stop(self):
        self.is_running = False
        # Do NOT set stop_event here because it stops the global pricing/balance worker
        # Instead, we just stop the TWMs and clear active trading accounts
        for i, acc in list(self.accounts.items()):
            try:
                if 'twm' in acc:
                    acc['twm'].stop()
                    # Small sleep to allow websocket cleanup and prevent fail_connection AttributeError
                    time.sleep(0.1)
            except Exception:
                pass
        self.accounts = {}
        self.log("bot_stopped", is_key=True)

    def _setup_strategy_for_account(self, idx, symbol):
        if idx not in self.accounts:
            return
        acc = self.accounts[idx]
        client = acc['client']
        strategy = self._get_strategy(idx, symbol)
        if not symbol: return

        try:
            # Get and cache exchange info centrally
            with self.market_data_lock:
                if symbol not in self.shared_market_data:
                    info = client.futures_exchange_info()
                    for s in info['symbols']:
                        if s['symbol'] == symbol:
                            self.shared_market_data[symbol] = {'info': s, 'price': 0.0, 'last_update': 0}
                            break
            
            # Set leverage and margin type
            leverage = int(strategy.get('leverage', 20))
            max_l = self.max_leverages.get(symbol, 125)
            if leverage > max_l:
                leverage = max_l
            margin_type = strategy.get('margin_type', 'CROSSED')

            try:
                client.futures_change_margin_type(symbol=symbol, marginType=margin_type)
            except BinanceAPIException as e:
                if "No need to change margin type" not in e.message:
                    self.log("margin_type_error", level='warning', account_name=acc['info'].get('name'), is_key=True, error=e.message)

            client.futures_change_leverage(symbol=symbol, leverage=leverage)

            self.log("leverage_set", account_name=acc['info'].get('name'), is_key=True, leverage=leverage, margin_type=margin_type)

            # Force metrics update to get balance
            self._update_account_metrics(idx, force=True)

            # Initial entry if needed
            self._check_and_place_initial_entry(idx, symbol)

        except Exception as e:
            self.log("strategy_setup_error", level='error', account_name=acc['info'].get('name'), is_key=True, error=str(e))

    def _check_and_place_initial_entry(self, idx, symbol):
        if idx not in self.accounts: return
        acc = self.accounts[idx]
        client = acc['client']
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG').upper()
        entry_type = strategy.get('entry_type', 'LIMIT').upper()
        
        trade_amount_val = float(strategy.get('trade_amount_usdc', 0))
        leverage = int(strategy.get('leverage', 20))
        is_pct = strategy.get('trade_amount_is_pct', False)
        
        with self.market_data_lock:
            current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
        
        if current_price <= 0: return
        
        entry_price = float(strategy.get('entry_price', 0))

        # Determine calculation price for quantity
        # For LIMIT/COND_LIMIT, use entry_price to ensure notional amount (qty*price) is exact.
        # For MARKET/COND_MARKET, use current market price.

        # VERY IMPORTANT: Price Mismatch Protection
        # If entry_price is wildly different from current_price (e.g. 5x difference),
        # it's likely a stale price inherited from a previous symbol in the UI (e.g. DOGE inheriting BTC price).
        # We MUST block this to avoid massive quantity errors.
        if entry_type in ['LIMIT', 'COND_LIMIT'] and entry_price > 0:
            # Increase tolerance to 5x for volatile assets, but keep protection
            if entry_price > current_price * 5 or entry_price < current_price * 0.2:
                log_key = f"stale_price_skip_{idx}_{symbol}"
                now = time.time()
                if now - self.last_log_times.get(log_key, 0) > 60:
                    self.log(f"Skipping {symbol} - Entry price {entry_price} is too far from market {current_price}. This is likely inherited from another symbol. Please re-select the symbol in the UI to refresh.", level='warning', account_name=acc['info'].get('name'))
                    self.last_log_times[log_key] = now
                return
            calc_price = entry_price
        else:
            calc_price = current_price

        # Calculate quantity for new entry
        balance = self.account_balances.get(idx, 0.0)
        if is_pct:
            quantity = (balance * (trade_amount_val / 100.0) * leverage) / calc_price
            calc_details = f"(Balance {balance:.2f} * {trade_amount_val}% * Lev {leverage}) / {calc_price}"
        else:
            quantity = (trade_amount_val * leverage) / calc_price
            calc_details = f"(Amount {trade_amount_val} * Lev {leverage}) / {calc_price}"

        # Debug log for quantity calculation
        log_key_calc = f"qty_calc_debug_{idx}_{symbol}"
        now = time.time()
        if now - self.last_log_times.get(log_key_calc, 0) > 300: # Every 5 mins
            logging.debug(f"[{acc['info'].get('name')}] Calculated quantity for {symbol}: {quantity} using {calc_details}")
            self.last_log_times[log_key_calc] = now

        # "Use Existing Assets" logic
        use_existing = strategy.get('use_existing', strategy.get('use_existing_assets', True))
        pos = client.futures_position_information(symbol=symbol)
        p_info = next((p for p in pos if p['symbol'] == symbol and float(p['positionAmt']) != 0), None)
        has_pos = p_info is not None

        if use_existing and has_pos:
            # Force grid placement if not already placed
            with self.data_lock:
                if (idx, symbol) not in self.grid_state:
                    self.log("pos_exists_skip", account_name=acc['info'].get('name'), is_key=True, symbol=symbol)
                    self.grid_state[(idx, symbol)] = {'initial_filled': True, 'levels': {}}
                    state = self.grid_state[(idx, symbol)]
                    # Only place TP grid if tp_enabled
                    if strategy.get('tp_enabled', True):
                        entry_p = float(p_info['entryPrice']) if float(p_info['entryPrice']) > 0 else current_price
                        state['avg_entry_price'] = entry_p
                        pos_qty = abs(float(p_info['positionAmt']))
                        actual_direction = 'LONG' if float(p_info['positionAmt']) > 0 else 'SHORT'

                        tp_targets = strategy.get('tp_targets', [])
                        if not tp_targets:
                            total_f = int(strategy.get('total_fractions', 8))
                            dev = float(strategy.get('price_deviation', 0.6))
                            tp_targets = [
                                {'percent': (i + 1) * dev, 'volume': 100.0 / total_f}
                                for i in range(total_f)
                            ]

                        self._setup_tp_targets(idx, symbol, entry_p, tp_targets, pos_qty, actual_direction)
            return

        if quantity <= 0 or entry_price <= 0: return

        # Check if we have open orders
        orders = client.futures_get_open_orders(symbol=symbol)
        
        if not has_pos:
            if orders:
                # Synchronization: check if order price matches config price
                # Only check for LIMIT entries
                if entry_type == 'LIMIT':
                    for o in orders:
                        if o['type'] == 'LIMIT' and o['side'] == (Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL):
                            order_price = float(o['price'])
                            # If price differs significantly or is just different, cancel it
                            if abs(order_price - entry_price) > 0.00000001:
                                self.log("price_mismatch_cancel", account_name=acc['info'].get('name'), is_key=True, symbol=symbol, old=order_price, new=entry_price)
                                client.futures_cancel_order(symbol=symbol, orderId=o['orderId'])
                                with self.data_lock:
                                    if (idx, symbol) in self.grid_state:
                                        del self.grid_state[(idx, symbol)]
                                return # Next loop will re-place
                return

            # Safety check: Prevent placing LIMIT orders too far from market price
            # to avoid spamming "Limit price can't be higher/lower than X" errors
            if entry_type == 'LIMIT':
                # Binance usually allows up to 5% or 10% deviation for futures
                # We'll use a conservative 5% check
                is_invalid = False
                if direction == 'LONG' and entry_price > current_price * 1.05:
                    is_invalid = True
                elif direction == 'SHORT' and entry_price < current_price * 0.95:
                    is_invalid = True

                if is_invalid:
                    log_key = f"price_out_of_range_{idx}_{symbol}"
                    now = time.time()
                    if now - self.last_log_times.get(log_key, 0) > 60:
                        self.log(f"Limit order price {entry_price} for {symbol} is likely invalid (Market: {current_price}). Skipping to avoid API error.", level='warning', account_name=acc['info'].get('name'))
                        self.last_log_times[log_key] = now
                    return

            side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
            
            if entry_type == 'MARKET':
                self.log("placing_market_initial", account_name=acc['info'].get('name'), is_key=True, direction=direction)
                self._execute_market_entry(idx, symbol)
                return

            if entry_type in ['CONDITIONAL', 'COND_LIMIT', 'COND_MARKET']:
                self.log("conditional_active", account_name=acc['info'].get('name'), is_key=True, symbol=symbol, trigger_price=entry_price)
                with self.data_lock:
                    self.grid_state[(idx, symbol)] = {
                        'conditional_active': True,
                        'conditional_type': entry_type,
                        'trigger_price': entry_price,
                        'initial_filled': False,
                        'levels': {}
                    }
                return

            if strategy.get('trailing_buy_enabled', False):
                self.log("trailing_buy_starting", account_name=acc['info'].get('name'), is_key=True, direction=direction, target_price=entry_price)
                with self.data_lock:
                    self.grid_state[(idx, symbol)] = {
                        'trailing_buy_active': True,
                        'trailing_buy_target': entry_price,
                        'trailing_buy_peak': 0, # Best price reached
                        'initial_filled': False,
                        'levels': {}
                    }
                return

            # Check balance before logging "placing_initial" to avoid log spam if empty
            # Use calc_price (current price for Market, entry price for Limit) for validation
            if not self._check_balance_for_order(idx, quantity, calc_price):
                log_key = f"insufficient_balance_{idx}_{symbol}"
                now = time.time()
                if now - self.last_log_times.get(log_key, 0) > 60:
                    self.log("insufficient_balance", level='warning', account_name=acc['info'].get('name'), is_key=True, qty=quantity, price=calc_price)
                    self.last_log_times[log_key] = now
                return

            self.log("placing_initial", account_name=acc['info'].get('name'), is_key=True, direction=direction, price=entry_price)
            try:
                order_id = self._place_limit_order(idx, symbol, side, quantity, entry_price)

                if order_id:
                    with self.data_lock:
                        self.grid_state[(idx, symbol)] = {
                            'initial_order_id': order_id,
                            'initial_filled': False,
                            'levels': {}
                        }
            except Exception as e:
                self.log("initial_failed", level='error', account_name=acc['info'].get('name'), is_key=True, error=str(e))

    def _format_quantity(self, symbol, quantity):
        with self.market_data_lock:
            info = self.shared_market_data.get(symbol, {}).get('info')
        if not info: return f"{quantity:.8f}".rstrip('0').rstrip('.')

        step_size = "0.00000001"
        for f in info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                step_size = f['stepSize']
                break

        step_d = Decimal(step_size).normalize()
        qty_d = Decimal(str(quantity))

        # Quantize quantity to step size
        # We floor to avoid 'insufficient balance' or 'quantity too high'
        result = (qty_d / step_d).quantize(Decimal('1'), rounding=ROUND_FLOOR) * step_d
        
        # Determine decimal places from step_size
        precision = max(0, -step_d.as_tuple().exponent)
        
        return format(result, f'.{precision}f')


    def _format_price(self, symbol, price):
        with self.market_data_lock:
            info = self.shared_market_data.get(symbol, {}).get('info')
        if not info: return f"{price:.8f}".rstrip('0').rstrip('.')

        tick_size = "0.00000001"
        for f in info['filters']:
            if f['filterType'] == 'PRICE_FILTER':
                tick_size = f['tickSize']
                break
        
        tick_d = Decimal(tick_size).normalize()
        price_d = Decimal(str(price))
        
        # Quantize to tick size
        result = (price_d / tick_d).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_d
        
        precision = max(0, -tick_d.as_tuple().exponent)
        return format(result, f'.{precision}f')

    def _handle_user_data(self, idx, msg):
        if not self.is_running or idx not in self.accounts:
            return
            
        event_type = msg.get('e')
        acc_name = self.accounts[idx]['info'].get('name')

        if event_type == 'ORDER_TRADE_UPDATE':
            order_data = msg.get('o', {})
            symbol = order_data.get('s')
            status = order_data.get('X')
            side = order_data.get('S')
            order_id = order_data.get('i')
            avg_price = float(order_data.get('ap', 0))
            filled_qty = float(order_data.get('z', 0))

            if status == 'FILLED':
                self.log("order_filled", account_name=acc_name, is_key=True, id=order_id, side=side, qty=filled_qty, symbol=symbol, price=avg_price)
                self._process_filled_order(idx, symbol, order_data)

        elif event_type == 'ACCOUNT_UPDATE':
            # Fast update for specific assets from WebSocket event
            update_data = msg.get('a', {})
            balances = update_data.get('B', [])
            for b in balances:
                if b.get('a') == 'USDC':
                    self.account_balances[idx] = float(b.get('wb', self.account_balances.get(idx, 0)))
            self.account_last_update[idx] = time.time()

            # Fast update for positions
            positions_data = update_data.get('P', [])
            if positions_data:
                new_positions = []
                for p in positions_data:
                    if float(p.get('pa', 0)) != 0:
                        new_positions.append({
                            'symbol': p['s'],
                            'amount': p['pa'],
                            'entryPrice': p['ep'],
                            'unrealizedProfit': p['up'],
                            'leverage': p['l']
                        })
                # Only update if we actually got position data to avoid clearing accidentally
                if new_positions:
                    self.open_positions[idx] = new_positions

            self._emit_account_update()

    def _process_filled_order(self, idx, symbol, order_data):
        order_id = order_data.get('i')
        strategy = self._get_strategy(idx, symbol)
        if not strategy: return

        direction = strategy.get('direction', 'LONG')
        total_fractions = int(strategy.get('total_fractions', 8))
        price_deviation = float(strategy.get('price_deviation', 0.6)) / 100.0
        
        # Calculate total_qty based on actual fill
        avg_price = float(order_data.get('ap', 0))
        filled_qty = float(order_data.get('z', 0))
        if avg_price <= 0 or filled_qty <= 0: return
        
        total_qty = filled_qty
        fraction_qty = total_qty / total_fractions
        entry_price_base = float(strategy.get('entry_price', 0))

        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state: return

            # 1. Initial Entry Filled
            if not state.get('initial_filled') and order_id == state.get('initial_order_id'):
                state['initial_filled'] = True
                state['avg_entry_price'] = avg_price
                state['tp_triggered'] = False 
                state['pending_reentry_qty'] = 0.0
                self.log("initial_filled_grid", account_name=self.accounts[idx]['info'].get('name'), is_key=True, price=avg_price)
                
                if strategy.get('tp_enabled', True):
                    tp_targets = strategy.get('tp_targets', [])
                    if not tp_targets:
                        # Fallback to default ladder (8 steps, 0.6% deviation as per client request)
                        total_f = int(strategy.get('total_fractions', 8))
                        dev = float(strategy.get('price_deviation', 0.6))
                        tp_targets = [
                            {'percent': (i + 1) * dev, 'volume': 100.0 / total_f}
                            for i in range(total_f)
                        ]
                        self.log(f"Using default {total_f}-step TP ladder for {symbol}", 'info')
                    
                    self._setup_tp_targets(idx, symbol, avg_price, tp_targets, total_qty, direction)
                return

            # 2. Check levels for TP fills
            for level, lvl_data in list(state['levels'].items()):
                if lvl_data.get('tp_order_id') and order_id == lvl_data.get('tp_order_id'):
                    qty_filled = lvl_data.get('qty', fraction_qty)
                    self.log("tp_filled_manual", account_name=self.accounts[idx]['info'].get('name'), is_key=True, level=level, qty=qty_filled)
                    lvl_data['filled'] = True
                    lvl_data['tp_order_id'] = None # Clear filled ID
                    
                    self._handle_reentry_logic(idx, symbol, qty_filled)
                    return

            # 3. Handle Re-entry Fill (Consolidated)
            if strategy.get('consolidated_reentry', True) and order_id == state.get('consolidated_reentry_id'):
                self.log("reentry_filled_tp_all", account_name=self.accounts[idx]['info'].get('name'), is_key=True)
                state['consolidated_reentry_id'] = None
                state['pending_reentry_qty'] = 0.0 # Reset pool
                
                # Update average entry price
                avg_price_fill = float(order_data.get('ap', 0))
                if avg_price_fill > 0:
                    state['avg_entry_price'] = avg_price_fill
                
                anchor = state.get('avg_entry_price', entry_price_base)
                # Re-place TPs that were filled
                for l, o in list(state['levels'].items()):
                    if o.get('tp_order_id') is None and not o.get('is_market') and not o.get('trailing_eligible'):
                        pct = o.get('percent', 0)
                        tp_price = anchor * (1 + pct) if direction == 'LONG' else anchor * (1 - pct)

                        # Update stored price
                        o['price'] = tp_price

                        level_qty = o.get('qty', fraction_qty)
                        tp_id = self._place_limit_order(idx, symbol, Client.SIDE_SELL if direction == 'LONG' else Client.SIDE_BUY, level_qty, tp_price)
                        o['tp_order_id'] = tp_id
                        o['filled'] = False
                return

    def _handle_reentry_logic(self, idx, symbol, qty_filled):
        """Handles the logic for placing/updating re-entry orders after a TP fill."""
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')
        entry_price_base = float(strategy.get('entry_price', 0))

        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state: return

            if strategy.get('consolidated_reentry', True):
                pending = state.get('pending_reentry_qty', 0.0)
                pending += qty_filled
                state['pending_reentry_qty'] = pending

                client = self.accounts[idx]['client']
                # Cancel existing re-entry order
                old_re_id = state.get('consolidated_reentry_id')
                if old_re_id:
                    try: client.futures_cancel_order(symbol=symbol, orderId=old_re_id)
                    except: pass

                # Place new consolidated re-entry at the original Entry Price
                anchor = state.get('avg_entry_price', entry_price_base)
                re_side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
                if self._check_balance_for_order(idx, pending, anchor):
                    new_re_id = self._place_limit_order(idx, symbol, re_side, pending, anchor)
                    state['consolidated_reentry_id'] = new_re_id
                    self.log("reentry_updated", account_name=self.accounts[idx]['info'].get('name'), is_key=True, qty=pending, price=anchor)

    def _setup_tp_targets(self, idx, symbol, entry_price, targets, total_qty, direction):
        state = self.grid_state.get((idx, symbol))
        if not state: return
        
        strategy = self._get_strategy(idx, symbol)
        tp_market_mode = strategy.get('tp_market_mode', False)
        
        state['levels'] = {} # Reset levels for new targets
        
        for i, target in enumerate(targets, 1):
            pct = float(target.get('percent', 0)) / 100.0
            volume_pct = float(target.get('volume', 0)) / 100.0
            qty = total_qty * volume_pct
            
            if direction == 'LONG':
                tp_price = entry_price * (1 + pct)
                side = Client.SIDE_SELL
            else:
                tp_price = entry_price * (1 - pct)
                side = Client.SIDE_BUY

            # Only the LAST target can have trailing if enabled
            is_last = (i == len(targets))
            trailing_eligible = is_last and strategy.get('trailing_tp_enabled', False)

            if tp_market_mode or trailing_eligible:
                order_id = None
            else:
                order_id = self._place_limit_order(idx, symbol, side, qty, tp_price)

            state['levels'][i] = {
                'tp_order_id': order_id,
                're_entry_order_id': None,
                'price': tp_price,
                'percent': pct,
                'qty': qty,
                'side': side,
                'is_market': tp_market_mode,
                'trailing_eligible': trailing_eligible,
                'filled': False
            }

    def _check_balance_for_order(self, idx, qty, price, leverage=None):
        # Specifically check USDC balance for USDC-M pairs
        balance = self.account_balances.get(idx, 0)
        notional = qty * price

        # If leverage is not provided, try to find it in config
        if leverage is None:
            leverage = 20 # Default fallback
            # We don't have symbol here, so this is an approximation.
            # In _place_limit_order we can pass it.

        # Margin required = Notional / Leverage
        # Add a 5% buffer for fees and price movements
        margin_required = (notional / leverage) * 1.05
        return balance >= margin_required

    def _place_limit_order(self, idx, symbol, side, qty, price):
        client = self.accounts[idx]['client']

        strategy = self._get_strategy(idx, symbol)
        leverage = int(strategy.get('leverage', 20))

        # Validate balance before placing re-buy/re-sell orders
        if not self._check_balance_for_order(idx, qty, price, leverage=leverage):
            log_key = f"insufficient_balance_{idx}_{symbol}"
            now = time.time()
            if now - self.last_log_times.get(log_key, 0) > 60: # Log at most once per minute per symbol
                self.log("insufficient_balance", level='warning', account_name=self.accounts[idx]['info'].get('name'), is_key=True, qty=qty, price=price)
                self.last_log_times[log_key] = now
            return None

        try:
            formatted_qty = self._format_quantity(symbol, qty)
            formatted_price = self._format_price(symbol, price)

            logging.debug(f"[{self.accounts[idx]['info'].get('name')}] Placing {side} LIMIT order for {symbol}: Qty {formatted_qty} @ Price {formatted_price}")

            # Validation for limit price vs market price to avoid common "Price out of range" errors
            with self.market_data_lock:
                current_market_price = self.shared_market_data.get(symbol, {}).get('price', 0)

            if current_market_price > 0:
                # Basic check: BUY limit cannot be much higher than market, SELL limit cannot be much lower.
                # Binance has a "Price Filter" and "Percent Price Filter"
                # If it's significantly off, we log it but try anyway, as Binance will reject it.
                pass

            order = client.futures_create_order(
                symbol=symbol,
                side=side,
                type=Client.FUTURE_ORDER_TYPE_LIMIT,
                timeInForce=Client.TIME_IN_FORCE_GTC,
                quantity=formatted_qty,
                price=formatted_price
            )
            return order['orderId']
        except BinanceAPIException as e:
            # Catch specific price out of range errors (including -4016 and -4025)
            if e.code in [-4016, -4025] or "Price out of range" in e.message or "Price higher than" in e.message or "Price lower than" in e.message:
                self.log("limit_order_price_error", level='warning', account_name=self.accounts[idx]['info'].get('name'), is_key=False, symbol=symbol, error=e.message)
            else:
                self.log("limit_order_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))
            return None
        except Exception as e:
            self.log("limit_order_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))
            return None

    def _update_account_metrics(self, idx, force=False):
        if idx not in self.accounts: return
        acc = self.accounts[idx]
        client = acc['client']
        try:
            # Throttle updates
            if not force and time.time() - acc['last_update'] < 10: return
            acc['last_update'] = time.time()

            account_info = client.futures_account()
            
            # Find USDC asset balance specifically
            usdc_found = False
            for asset in account_info.get('assets', []):
                if asset['asset'] == 'USDC':
                    self.account_balances[idx] = float(asset['walletBalance'])
                    usdc_found = True
                    break
            
            # If not found in assets, don't zero it out, maybe it's zero or network issue
            if not usdc_found and idx not in self.account_balances:
                self.account_balances[idx] = 0.0

            self.account_last_update[idx] = time.time()

            total_unrealized_pnl = float(account_info['totalUnrealizedProfit'])

            positions = []
            for p in account_info['positions']:
                if float(p['positionAmt']) != 0:
                    positions.append({
                        'symbol': p['symbol'],
                        'amount': p['positionAmt'],
                        'entryPrice': p['entryPrice'],
                        'unrealizedProfit': p['unrealizedProfit'],
                        'leverage': p['leverage']
                    })
            self.open_positions[idx] = positions

            self._emit_account_update()

        except BinanceAPIException as e:
            if e.code == -2015:
                self.account_errors[idx] = "Invalid API Key/Permissions"
                log_key = f"invalid_api_{idx}"
                now = time.time()
                if now - self.last_log_times.get(log_key, 0) > 300:
                    self.log("invalid_api_keys", level='error', account_name=acc['info'].get('name'), is_key=True, ip=self.server_ip)
                    self.last_log_times[log_key] = now
            else:
                logging.error(f"Error updating metrics for account {idx}: {e}")
        except Exception as e:
            logging.error(f"Error updating metrics for account {idx}: {e}")

    def _update_bg_account_metrics(self, idx):
        """Updates balance and positions for background accounts (non-trading)."""
        acc = self.bg_clients.get(idx)
        if not acc: return
        client = acc['client']
        try:
            account_info = client.futures_account()
            # Find USDC asset balance specifically
            usdc_balance = 0.0
            for asset in account_info.get('assets', []):
                if asset['asset'] == 'USDC':
                    usdc_balance = float(asset['walletBalance'])
                    break
            self.account_balances[idx] = usdc_balance
            self.account_errors[idx] = None

            positions = []
            for p in account_info['positions']:
                if float(p['positionAmt']) != 0:
                    positions.append({
                        'symbol': p['symbol'],
                        'amount': p['positionAmt'],
                        'entryPrice': p['entryPrice'],
                        'unrealizedProfit': p['unrealizedProfit'],
                        'leverage': p['leverage']
                    })
            self.open_positions[idx] = positions
        except BinanceAPIException as e:
            if e.code == -2015:
                self.account_errors[idx] = "Invalid API Key/Permissions"
                # Don't spam but keep track
                pass
            else:
                # logging.debug(f"Error updating bg metrics for account {idx}: {e}")
                pass
        except Exception as e:
            # logging.error(f"Error updating bg metrics for account {idx}: {e}")
            pass

    def _emit_account_update(self):
        total_balance = sum(list(self.account_balances.values()))
        total_pnl = 0.0
        
        all_positions = []
        manual_positions = []
        
        # Use a copy of indices to avoid thread issues
        all_idxs = set(list(self.account_balances.keys()) + list(self.open_positions.keys()))

        for idx in all_idxs:
            pos_list = self.open_positions.get(idx, [])
            api_accounts = self.config.get('api_accounts', [])
            acc_name = api_accounts[idx].get('name', f"Account {idx+1}") if idx < len(api_accounts) else f"Account {idx+1}"

            for p in pos_list:
                p_copy = p.copy()
                p_copy['account'] = acc_name
                p_copy['account_idx'] = idx
                total_pnl += float(p_copy.get('unrealizedProfit', 0))
                
                symbol = p_copy['symbol']
                with self.data_lock:
                    state = self.grid_state.get((idx, symbol))
                p_copy['is_manual'] = (state is None)
                
                all_positions.append(p_copy)
                if p_copy['is_manual']:
                    manual_positions.append(p_copy)

        payload = {
            'total_balance': total_balance,
            'total_equity': total_balance + total_pnl,
            'total_pnl': total_pnl,
            'positions': all_positions,
            'manual_positions': manual_positions,
            'running': self.is_running,
            'accounts': [
            {
                'name': self.config.get('api_accounts', [])[idx].get('name', f"Account {idx+1}") if idx < len(self.config.get('api_accounts', [])) else f"Account {idx+1}",
                'balance': self.account_balances.get(idx, 0.0),
                'active': idx in self.accounts,
                'has_client': idx in self.bg_clients,
                'error': self.account_errors.get(idx),
                'last_update': self.account_last_update.get(idx, 0)
            } for idx in range(len(self.config.get('api_accounts', [])))
        ]
        }
        self.emit('account_update', payload)

    def apply_live_config_update(self, new_config):
        old_config = self.config
        self.config = new_config

        # Handle Language Change
        lang_changed = old_config.get('language') != self.config.get('language')
        if lang_changed:
            self.language = self.config.get('language', 'pt-BR')
            # Update all existing logs in the queue
            for entry in self.console_logs:
                entry['rendered'] = self._render_log(entry)
            # Re-emit the whole status to refresh UI text
            self.emit('bot_status', {'running': self.is_running})
            self.emit('clear_console', {})
            for log in list(self.console_logs):
                self.emit('console_log', log)

        # Check if is_demo changed
        demo_changed = old_config.get('is_demo') != self.config.get('is_demo')

        if demo_changed:
            mode_str = "DEMO (Testnet)" if self.config.get('is_demo') else "LIVE (Mainnet)"
            self.log(f"Switching to {mode_str} mode. Clearing caches...", level='warning')

            # Clear all environment-specific data
            with self.market_data_lock:
                self.shared_market_data = {}
                self.max_leverages = {}

            with self.data_lock:
                self.grid_state = {}
                self.trailing_state = {}

            self.account_balances = {}
            self.open_positions = {}

        # Immediate refresh of background states
        self._initialize_bg_clients()
        self._initialize_market_ws()
        self.metadata_client = self._get_metadata_client()
        self.market_client = self._create_client("", "")

        # Cleanup stale data for removed accounts or cleared keys
        num_accounts = len(self.config.get('api_accounts', []))
        api_accounts = self.config.get('api_accounts', [])

        for idx in list(self.account_balances.keys()):
            if idx >= num_accounts:
                del self.account_balances[idx]
            else:
                acc = api_accounts[idx]
                if not acc.get('api_key') or not acc.get('api_secret'):
                    del self.account_balances[idx]

        for idx in list(self.open_positions.keys()):
            if idx >= num_accounts:
                del self.open_positions[idx]
            else:
                acc = api_accounts[idx]
                if not acc.get('api_key') or not acc.get('api_secret'):
                    del self.open_positions[idx]

        self._emit_account_update()
        
        if self.is_running:
            if demo_changed:
                self.stop()
                self.start()
                return {"success": True}

            api_accounts = self.config.get('api_accounts', [])
            symbols = self.config.get('symbols', [])
            
            # Handle account changes (Surgical updates)
            active_idxs = list(self.accounts.keys())
            for idx in active_idxs:
                if idx >= len(api_accounts):
                    try:
                        if 'twm' in self.accounts[idx]:
                            self.accounts[idx]['twm'].stop()
                            time.sleep(0.1)
                    except Exception as e:
                        logging.debug(f"Error stopping TWM for account {idx}: {e}")
                    del self.accounts[idx]
                    with self.data_lock:
                        for key in list(self.grid_state.keys()):
                            if key[0] == idx: del self.grid_state[key]
                        for key in list(self.symbol_threads.keys()):
                            if key[0] == idx: del self.symbol_threads[key]

            for i, acc_config in enumerate(api_accounts):
                enabled = acc_config.get('enabled', True)
                api_key = acc_config.get('api_key', '').strip()
                api_secret = acc_config.get('api_secret', '').strip()
                has_keys = api_key and api_secret

                if i in self.accounts:
                    old_acc_config = self.accounts[i]['info']
                    keys_changed = (old_acc_config.get('api_key', '').strip() != api_key or
                                    old_acc_config.get('api_secret', '').strip() != api_secret)

                    if not enabled or not has_keys or keys_changed:
                        try:
                            if 'twm' in self.accounts[i]:
                                self.accounts[i]['twm'].stop()
                                time.sleep(0.1)
                        except Exception as e:
                            logging.debug(f"Error stopping TWM for account {i}: {e}")
                        del self.accounts[i]
                        if i in self.account_balances: del self.account_balances[i]
                        if i in self.open_positions: del self.open_positions[i]
                        with self.data_lock:
                            for key in list(self.grid_state.keys()):
                                if key[0] == i: del self.grid_state[key]
                            for key in list(self.symbol_threads.keys()):
                                if key[0] == i: del self.symbol_threads[key]
                    else:
                        self.accounts[i]['info'] = acc_config

                if i not in self.accounts and enabled and has_keys:
                    self._init_account(i, acc_config)

            for idx in self.accounts:
                # 1. Start threads for new symbols
                for symbol in symbols:
                    self._start_symbol_thread(idx, symbol)
                
                # 2. Update leverage for all active symbols
                for symbol in symbols:
                    strategy = self._get_strategy(idx, symbol)
                    if strategy:
                        leverage = int(strategy.get('leverage', 20))
                        try:
                            # Clamp to max allowed
                            max_l = self.max_leverages.get(symbol, 125)
                            if leverage > max_l: leverage = max_l
                            
                            self.accounts[idx]['client'].futures_change_leverage(symbol=symbol, leverage=leverage)
                            # self.log("live_update_leverage", account_name=self.accounts[idx]['info'].get('name'), is_key=True, leverage=leverage, symbol=symbol)
                        except Exception as e:
                            pass
        
        return {"success": True}

    def close_position(self, account_idx, symbol):
        # Find the client in trading accounts or background clients
        target_client = None
        if isinstance(account_idx, int):
            if account_idx in self.accounts:
                target_client = self.accounts[account_idx]['client']
            elif account_idx in self.bg_clients:
                target_client = self.bg_clients[account_idx]['client']
        else:
            # Fallback for name-based lookup
            for acc in list(self.accounts.values()):
                if acc['info'].get('name') == account_idx:
                    target_client = acc['client']
                    break
            if not target_client:
                for acc in list(self.bg_clients.values()):
                    if acc.get('name') == account_idx:
                        target_client = acc['client']
                        break

        if target_client:
            try:
                # Cancel all orders
                target_client.futures_cancel_all_open_orders(symbol=symbol)
                # Close position by market order
                pos = target_client.futures_position_information(symbol=symbol)
                for p in pos:
                    if p['symbol'] == symbol:
                        amt = float(p['positionAmt'])
                        if amt != 0:
                            side = Client.SIDE_SELL if amt > 0 else Client.SIDE_BUY
                            target_client.futures_create_order(
                                symbol=symbol,
                                side=side,
                                type=Client.FUTURE_ORDER_TYPE_MARKET,
                                quantity=self._format_quantity(symbol, abs(amt))
                            )
                acc_name = self.config.get('api_accounts', [])[account_idx].get('name') if isinstance(account_idx, int) and account_idx < len(self.config.get('api_accounts', [])) else str(account_idx)
                self.log("pos_closed_manual", account_name=acc_name, is_key=True, symbol=symbol)

                # Cleanup grid state if managed
                with self.data_lock:
                    if isinstance(account_idx, int):
                        if (account_idx, symbol) in self.grid_state:
                            del self.grid_state[(account_idx, symbol)]
                    else:
                        for (idx, sym), state in list(self.grid_state.items()):
                            if sym == symbol:
                                api_accounts = self.config.get('api_accounts', [])
                                a_name = api_accounts[idx].get('name') if idx < len(api_accounts) else None
                                if a_name == account_idx:
                                    del self.grid_state[(idx, sym)]

            except Exception as e:
                self.log("error_closing_pos", level='error', account_name=str(account_idx), is_key=True, error=str(e))

    def _global_background_worker(self):
        """Global worker for fetching prices once and updating shared metrics."""
        while not self.stop_event.is_set():
            try:
                # 1. Update shared market data (prices)
                # Ensure we use a fresh list of symbols from the live config
                symbols = list(self.config.get('symbols', []))
                
                # Use metadata client or first active client for non-public metadata if needed
                active_client = None
                for acc in list(self.accounts.values()):
                    if acc.get('client'):
                        active_client = acc['client']
                        break
                if not active_client:
                    active_client = self.metadata_client

                if symbols:
                    # Optimize: Check if we need to fetch exchange info for any missing symbols
                    missing_info = [s for s in symbols if s not in self.shared_market_data or 'info' not in self.shared_market_data[s]]
                    if missing_info:
                        try:
                            # exchange_info is public, but we'll use market_client
                            ex_info = self.market_client.futures_exchange_info()
                            with self.market_data_lock:
                                for s_data in ex_info['symbols']:
                                    s_name = s_data['symbol']
                                    if s_name in symbols:
                                        if s_name not in self.shared_market_data:
                                            self.shared_market_data[s_name] = {'price': 0, 'last_update': 0}
                                        self.shared_market_data[s_name]['info'] = s_data
                        except Exception as e:
                            logging.error(f"Error fetching exchange info: {e}")

                    # 1a. WebSocket Health Check & Fallback
                    now = time.time()
                    price_map_emit = {}
                    with self.market_data_lock:
                        for symbol in symbols:
                            data = self.shared_market_data.get(symbol)
                            # If no data or stale (>10s), try polling
                            if not data or (now - data.get('last_update', 0) > 10):
                                try:
                                    ticker = self.market_client.futures_symbol_ticker(symbol=symbol)
                                    if symbol not in self.shared_market_data:
                                        self.shared_market_data[symbol] = {'price': 0, 'last_update': 0}
                                    data = self.shared_market_data[symbol]
                                    data['price'] = float(ticker['price'])
                                    data['last_update'] = now
                                except: pass

                            if data and 'price' in data:
                                price_map_emit[symbol] = {
                                    'bid': data.get('bid', data['price']),
                                    'ask': data.get('ask', data['price']),
                                    'last': data['price']
                                }
                    
                    if price_map_emit:
                        self.emit('price_update', price_map_emit)

                    # 1b. Fetch max leverage if not cached - Needs authentication
                    for symbol in symbols:
                        if symbol not in self.max_leverages:
                            try:
                                # Try active_client (first authenticated client)
                                if active_client:
                                    # This call needs authentication and can fail with -2015
                                    brackets = active_client.futures_leverage_bracket(symbol=symbol)
                                    if brackets and len(brackets) > 0:
                                        bracket_info = brackets[0] if 'brackets' in brackets[0] else brackets
                                        max_l = bracket_info['brackets'][0]['initialLeverage']
                                        self.max_leverages[symbol] = max_l
                            except Exception:
                                pass
                    
                    # Log warnings for symbols not found (typos or unsupported)
                    for s in symbols:
                        if s not in price_map_emit:
                            log_key = f"symbol_not_found_{s}"
                            now = time.time()
                            if now - self.last_log_times.get(log_key, 0) > 300: # Every 5 mins
                                self.log(f"Warning: Symbol {s} not found on Binance. Please check if it is a valid USDC pair.", level='warning')
                                self.last_log_times[log_key] = now

                    # 2. Update balances for all background accounts
                    for idx in list(self.bg_clients.keys()):
                        try:
                            self._update_bg_account_metrics(idx)
                        except Exception:
                            pass
                    
                    self._emit_account_update()
                    self.emit('max_leverages', self.max_leverages)
                
                # 3. Update active trading account metrics
                for idx in list(self.accounts.keys()):
                    try:
                        self._update_account_metrics(idx)
                    except Exception:
                        pass
                    
                time.sleep(1) # Faster update loop (1s)
            except Exception as e:
                # logging.error(f"Global worker error: {e}")
                time.sleep(2)

    def _emit_latest_prices(self):
        """Broadcasts the current last-known state from shared memory."""
        with self.market_data_lock:
            # Reconstruct price map from shared storage with full {bid, ask, last}
            price_map = {}
            for s, data in list(self.shared_market_data.items()):
                price_map[s] = {
                    'bid': data.get('bid', data['price']),
                    'ask': data.get('ask', data['price']),
                    'last': data['price']
                }
            if price_map:
                self.emit('price_update', price_map)
            self.emit('max_leverages', self.max_leverages)

    def _start_symbol_thread(self, idx, symbol):
        key = (idx, symbol)
        with self.data_lock:
            if key in self.symbol_threads and not self.symbol_threads[key].is_alive():
                del self.symbol_threads[key]

            if key not in self.symbol_threads:
                t = threading.Thread(target=self._symbol_logic_worker, args=(idx, symbol), daemon=True)
                self.symbol_threads[key] = t
                t.start()
                self.log("started_thread", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol)

    def _symbol_logic_worker(self, idx, symbol):
        """Dedicated worker for each symbol's grid and trailing logic."""
        try:
            if idx not in self.accounts: return
            current_client = self.accounts[idx].get('client')
            self._setup_strategy_for_account(idx, symbol)

            while self.is_running and not self.stop_event.is_set():
                if idx not in self.accounts or self.accounts[idx].get('client') != current_client:
                    break
                try:
                    # Check if this symbol still in config (for live removal)
                    if symbol not in self.config.get('symbols', []):
                        self.log("stopping_thread", is_key=True, symbol=symbol)
                        break
                    # 2. Check and place initial entry
                    self._check_and_place_initial_entry(idx, symbol)

                    # 3. Trailing Take Profit Logic
                    self._trailing_tp_logic(idx, symbol)

                    # 4. Market Take Profit Logic (if enabled)
                    self._tp_market_logic(idx, symbol)

                    # 5. Stop Loss Logic
                    self._stop_loss_logic(idx, symbol)
                    self._trailing_buy_logic(idx, symbol)
                    self._conditional_logic(idx, symbol)
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"Symbol logic worker error ({symbol}): {e}")
                    time.sleep(5)
        finally:
            with self.data_lock:
                key = (idx, symbol)
                if self.symbol_threads.get(key) == threading.current_thread():
                    del self.symbol_threads[key]


    def _trailing_buy_logic(self, idx, symbol):
        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state or not state.get('trailing_buy_active'): return
            
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')
        target_price = state.get('trailing_buy_target')
        dev_pct = float(strategy.get('trailing_buy_deviation', 0.1))

        with self.market_data_lock:
            current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
        
        if current_price <= 0: return

        # For LONG trailing buy: price must first hit target, then we track the LOWEST price, then we buy when it bounces up by dev_pct.
        # But 3Commas "Trailing Buy" usually means: price is BELOw target, we track LOW, then buy on BOUNCE.
        if direction == 'LONG':
            # Phase 1: Hit target (or if already below)
            if current_price <= target_price:
                if state['trailing_buy_peak'] == 0 or current_price < state['trailing_buy_peak']:
                    state['trailing_buy_peak'] = current_price
                
                # Check for bounce
                retrace = (current_price - state['trailing_buy_peak']) / state['trailing_buy_peak'] * 100
                if retrace >= dev_pct:
                    self.log("trailing_buy_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, bounce=f"{retrace:.2f}", price=current_price)
                    state['trailing_buy_active'] = False
                    self._execute_market_entry(idx, symbol)
        else: # SHORT
            if current_price >= target_price:
                if state['trailing_buy_peak'] == 0 or current_price > state['trailing_buy_peak']:
                    state['trailing_buy_peak'] = current_price
                
                # Check for dip
                retrace = (state['trailing_buy_peak'] - current_price) / state['trailing_buy_peak'] * 100
                if retrace >= dev_pct:
                    self.log("trailing_sell_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, dip=f"{retrace:.2f}", price=current_price)
                    state['trailing_buy_active'] = False
                    self._execute_market_entry(idx, symbol)

    def _conditional_logic(self, idx, symbol):
        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state or not state.get('conditional_active'): return
            
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')
        trigger_price = state.get('trigger_price', 0)
        cond_type = state.get('conditional_type', 'CONDITIONAL')

        with self.market_data_lock:
            current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
        
        if current_price <= 0: return

        # Trigger logic
        triggered = False
        if direction == 'LONG':
            if current_price >= trigger_price: triggered = True
        else: # SHORT
            if current_price <= trigger_price: triggered = True

        if triggered:
            self.log("conditional_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=current_price)
            with self.data_lock:
                state['conditional_active'] = False
            
            if cond_type == 'COND_MARKET' or cond_type == 'CONDITIONAL':
                self._execute_market_entry(idx, symbol)
            else: # COND_LIMIT
                order_price = trigger_price # Use same price for trigger and execution
                trade_amount_usdc = float(strategy.get('trade_amount_usdc', 0))
                leverage = int(strategy.get('leverage', 20))
                quantity = (trade_amount_usdc * leverage) / order_price
                side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
                
                try:
                    order_id = self._place_limit_order(idx, symbol, side, quantity, order_price)
                    if order_id:
                        with self.data_lock:
                            state['initial_order_id'] = order_id
                except Exception as e:
                    self.log("cond_limit_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))

    def _execute_market_entry(self, idx, symbol):
        strategy = self._get_strategy(idx, symbol)
        trade_amount_usdc = float(strategy.get('trade_amount_usdc', 0))
        leverage = int(strategy.get('leverage', 20))
        direction = strategy.get('direction', 'LONG')
        
        with self.market_data_lock:
            current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
        
        quantity = (trade_amount_usdc * leverage) / current_price
        side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
        
        if not self._check_balance_for_order(idx, quantity, current_price):
            log_key = f"insufficient_balance_{idx}_{symbol}"
            now = time.time()
            if now - self.last_log_times.get(log_key, 0) > 60:
                self.log("insufficient_balance", level='warning', account_name=self.accounts[idx]['info'].get('name'), is_key=True, qty=quantity, price=current_price)
                self.last_log_times[log_key] = now
            return

        try:
            self.log("executing_market_entry", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, direction=direction)
            # We use market order for trailing buy trigger
            order = self.accounts[idx]['client'].futures_create_order(
                symbol=symbol,
                side=side,
                type=Client.FUTURE_ORDER_TYPE_MARKET,
                quantity=self._format_quantity(symbol, quantity)
            )
            # The user data handler will catch the fill and place the grid
        except Exception as e:
            self.log("market_entry_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))

    def _tp_market_logic(self, idx, symbol):
        """Monitors price for manual market TP execution."""
        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state or not state.get('initial_filled'): return
            
            levels = state.get('levels', {})
            if not levels: return

        with self.market_data_lock:
            current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
        
        if current_price <= 0: return

        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')

        to_execute = []
        with self.data_lock:
            for lvl_idx, lvl in list(levels.items()):
                if not lvl.get('tp_order_id') and not lvl.get('filled') and not lvl.get('trailing_eligible'):
                    target_price = lvl['price']
                    triggered = False
                    if direction == 'LONG':
                        if current_price >= target_price: triggered = True
                    else: # SHORT
                        if current_price <= target_price: triggered = True
                    
                    if triggered:
                        to_execute.append((lvl_idx, lvl))

        for lvl_idx, lvl in to_execute:
            self.log("tp_market_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, level=lvl_idx, price=current_price)
            # Execute Market Order
            success = self._execute_market_close_partial(idx, symbol, lvl['qty'], lvl['side'])
            if success:
                with self.data_lock:
                    lvl['filled'] = True
                    self.log("tp_filled_market", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, level=lvl_idx)

                self._handle_reentry_logic(idx, symbol, lvl['qty'])

    def _execute_market_close_partial(self, idx, symbol, qty, side):
        """Executes a market order to close part of a position."""
        acc = self.accounts[idx]
        client = acc['client']
        try:
            client.futures_create_order(
                symbol=symbol,
                side=side,
                type=Client.ORDER_TYPE_MARKET,
                quantity=self._format_quantity(symbol, qty)
            )
            return True
        except Exception as e:
            self.log("tp_market_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))
            return False

    def _stop_loss_logic(self, idx, symbol):
        strategy = self._get_strategy(idx, symbol)
        if not strategy.get('stop_loss_enabled'): return
        
        sl_price = float(strategy.get('stop_loss_price', 0))
        if sl_price <= 0: return

        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state or not state.get('initial_filled'): return
            
            with self.market_data_lock:
                current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
            
            if current_price == 0: return
            
            direction = strategy.get('direction', 'LONG')
            
            # Trailing Stop Loss Logic
            if strategy.get('trailing_sl_enabled'):
                peak_key = (idx, symbol, 'sl_peak')
                if peak_key not in self.trailing_state:
                    self.trailing_state[peak_key] = {'peak': current_price}
                
                peak = self.trailing_state[peak_key]['peak']
                if direction == 'LONG':
                    if current_price > peak:
                        diff = current_price - peak
                        sl_price += diff # Move SL up with price
                        self.trailing_state[peak_key]['peak'] = current_price
                        strategy['stop_loss_price'] = sl_price
                else:
                    if current_price < peak:
                        diff = peak - current_price
                        sl_price -= diff # Move SL down with price
                        self.trailing_state[peak_key]['peak'] = current_price
                        strategy['stop_loss_price'] = sl_price

            # Move to Breakeven Logic
            if strategy.get('move_to_breakeven'):
                anchor = state.get('avg_entry_price', float(strategy.get('entry_price', 0)))
                if direction == 'LONG' and current_price > anchor * 1.005: # 0.5% in profit
                     if sl_price < anchor:
                         sl_price = anchor
                         strategy['stop_loss_price'] = sl_price
                elif direction == 'SHORT' and current_price < anchor * 0.995:
                     if sl_price > anchor:
                         sl_price = anchor
                         strategy['stop_loss_price'] = sl_price

            # Stop Loss Timeout Logic
            triggered = (direction == 'LONG' and current_price <= sl_price) or \
                        (direction == 'SHORT' and current_price >= sl_price)
            
            if triggered:
                if strategy.get('sl_timeout_enabled'):
                    timeout_sec = int(strategy.get('sl_timeout_duration', 10))
                    trigger_key = (idx, symbol, 'sl_trigger_time')
                    if trigger_key not in self.trailing_state:
                        self.trailing_state[trigger_key] = time.time()
                        self.log("sl_timeout_started", account_name=self.accounts[idx]['info'].get('name'), is_key=False, sec=timeout_sec)
                        return # Wait for next loop
                    
                    elapsed = time.time() - self.trailing_state[trigger_key]
                    if elapsed < timeout_sec:
                        return # Still waiting
                    
                    # If we are here, timeout reached. Close only if still triggered.
                
                self.log("stop_loss_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=current_price)
                self.close_position(self.accounts[idx]['info'].get('name'), symbol)
                state['initial_filled'] = False
                # Clean up trigger time
                trigger_key = (idx, symbol, 'sl_trigger_time')
                if trigger_key in self.trailing_state: del self.trailing_state[trigger_key]
            else:
                # Price recovered, reset timeout if any
                trigger_key = (idx, symbol, 'sl_trigger_time')
                if trigger_key in self.trailing_state: 
                    del self.trailing_state[trigger_key]
                    self.log("sl_timeout_reset", account_name=self.accounts[idx]['info'].get('name'), is_key=False)

    def _trailing_tp_logic(self, idx, symbol):
        strategy = self._get_strategy(idx, symbol)
        if not strategy.get('trailing_tp_enabled'): return

        with self.data_lock:
            state = self.grid_state.get((idx, symbol))
            if not state or not state.get('initial_filled'): return
            
            with self.market_data_lock:
                current_price = self.shared_market_data.get(symbol, {}).get('price', 0)
            
            if current_price == 0: return
            
            direction = strategy.get('direction', 'LONG')
            deviation = float(strategy.get('trailing_deviation', 0.5)) / 100.0
            anchor = state.get('avg_entry_price', float(strategy.get('entry_price', 0)))
            
            # Start trailing only if in profit by at least 0.1%
            profit_pct = (current_price - anchor) / anchor if direction == 'LONG' else (anchor - current_price) / anchor
            
            peak_key = (idx, symbol, 'tp_peak')
            if not state.get('tp_trailing_active'):
                if profit_pct > 0.001: 
                     state['tp_trailing_active'] = True
                     self.trailing_state[peak_key] = current_price
                     self.log("trailing_tp_tracking", account_name=self.accounts[idx]['info'].get('name'), is_key=False, symbol=symbol)
                return

            # Update peak
            peak = self.trailing_state.get(peak_key, current_price)
            if direction == 'LONG':
                if current_price > peak:
                    self.trailing_state[peak_key] = current_price
                elif current_price <= peak * (1 - deviation):
                    self.log("trailing_tp_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=current_price)
                    self.close_position(self.accounts[idx]['info'].get('name'), symbol)
                    state['initial_filled'] = False
                    state['tp_trailing_active'] = False
                    if peak_key in self.trailing_state: del self.trailing_state[peak_key]
            else:
                if current_price < peak:
                    self.trailing_state[peak_key] = current_price
                elif current_price >= peak * (1 + deviation):
                    self.log("trailing_tp_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=current_price)
                    self.close_position(self.accounts[idx]['info'].get('name'), symbol)
                    state['initial_filled'] = False
                    state['tp_trailing_active'] = False
                    if peak_key in self.trailing_state: del self.trailing_state[peak_key]

    def get_status(self):
        return {
            'running': self.is_running,
            'accounts_count': len(self.accounts),
            'total_balance': sum(list(self.account_balances.values()))
        }
