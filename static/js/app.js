const socket = io();

let currentLang = 'en-US';
let currentConfig = null;
let isBotRunning = false;
let activeSymbol = null;
let maxLeverages = {};
let currentBalance = 0; // Total equity for hero

document.addEventListener('DOMContentLoaded', () => {
    // Load translations from hidden div
    const transData = document.getElementById('translations-data');
    if (transData) {
        window.allTranslations = JSON.parse(transData.textContent);
    }
    loadConfig();
    setupEventListeners();
    setupSocketListeners();
});

function setupEventListeners() {
    // ... existing ...
    document.getElementById('lang-select').addEventListener('change', (e) => {
        currentLang = e.target.value;
        saveLiveConfig({ language: currentLang });
        applyUiTranslations();
    });

    document.getElementById('addNewSymbolBtn').addEventListener('click', async () => {
        const inputStr = document.getElementById('newSymbolInput').value;
        const symbol = inputStr ? inputStr.trim().toUpperCase() : '';
        if (symbol && symbol.length > 3) {
            if (!currentConfig.symbols.includes(symbol)) {
                currentConfig.symbols.push(symbol);
                // Copy current strategy or initialize a default one
                let newStrat = JSON.parse(JSON.stringify(currentConfig.symbol_strategies[activeSymbol] || {}));

                // Reset price-sensitive fields for the new symbol
                newStrat.entry_price = 0;
                newStrat.stop_loss_price = 0;
                newStrat.sl_order_price = 0;

                // Ensure the 8-step TP ladder and recycling are enabled by default for new symbols
                newStrat.tp_enabled = true;
                newStrat.consolidated_reentry = true;
                if (!newStrat.tp_targets || newStrat.tp_targets.length === 0) {
                    newStrat.total_fractions = 8;
                    newStrat.price_deviation = 0.6;
                    newStrat.tp_targets = Array.from({ length: 8 }, (_, i) => ({
                        percent: ((i + 1) * 0.6).toFixed(1),
                        volume: 12.5
                    }));
                }

                currentConfig.symbol_strategies[symbol] = newStrat;
                await saveLiveConfig();
                initSymbolPicker();
                renderSymbolsInModal();
                document.getElementById('newSymbolInput').value = '';
                document.getElementById('selectActiveSymbol').value = symbol;
                activeSymbol = symbol;
                updateUIFromConfig();
            }
        }
    });


    // ... balance % buttons ...
    document.getElementById('startStopBtn').addEventListener('click', () => {
        const btn = document.getElementById('startStopBtn');
        const spinner = document.getElementById('startStopSpinner');
        btn.disabled = true;
        if (spinner) spinner.classList.remove('d-none');

        if (isBotRunning) socket.emit('stop_bot');
        else socket.emit('start_bot');
    });

    // Asset Switcher
    document.getElementById('selectActiveSymbol').addEventListener('change', (e) => {
        activeSymbol = e.target.value;
        const unit = 'USDC';
        const labels = ['asset-symbol-label', 'base-asset-label', 'entry-price-asset-label', 'tp-price-asset-label', 'sl-price-asset-label', 'sl-order-price-asset-label'];
        labels.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerText = unit;
        });

        // Clear stale price display immediately
        document.getElementById('bid-price').innerText = '0.00';
        document.getElementById('ask-price').innerText = '0.00';

        updateUIFromConfig();
    });

    // Balance % Buttons
    document.querySelectorAll('#pct-buttons button').forEach(btn => {
        btn.addEventListener('click', () => {
            const pct = parseInt(btn.dataset.pct);
            const mode = document.getElementById('selectTradeAmountMode').value;

            if (mode === 'pct') {
                // In percentage mode, set the % value directly
                document.getElementById('inputTradeAmountUSDC').value = pct;
                updateStrategyField('trade_amount_usdc', pct);
            } else {
                // In fixed mode, calculate based on current total equity (for UX display)
                const totalEquity = currentBalance || 0;
                const amount = (totalEquity * (pct / 100)).toFixed(2);
                document.getElementById('inputTradeAmountUSDC').value = amount;
                updateStrategyField('trade_amount_usdc', parseFloat(amount));
            }

            updateTotalBaseUnits();
            document.querySelectorAll('#pct-buttons button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        });
    });

    document.getElementById('selectTradeAmountMode').addEventListener('change', (e) => {
        const mode = e.target.value;
        const isPct = (mode === 'pct');
        updateStrategyField('trade_amount_is_pct', isPct);

        // Update labels
        document.getElementById('asset-symbol-label').classList.toggle('d-none', isPct);
        document.getElementById('pct-symbol-label').classList.toggle('d-none', !isPct);

        updateTotalBaseUnits();
    });

    // Amount & Entry Updates
    document.getElementById('inputTradeAmountUSDC').addEventListener('input', () => {
        updateStrategyField('trade_amount_usdc', parseFloat(document.getElementById('inputTradeAmountUSDC').value));
        updateTotalBaseUnits();
    });
    document.getElementById('inputEntryPrice').addEventListener('input', updateTotalBaseUnits);

    // Pillar Toggles & Overlays
    document.getElementById('tpToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('tp-overlay').classList.toggle('d-none', checked);
        updateStrategyField('tp_enabled', checked);
    });

    document.getElementById('btn-enable-tp').addEventListener('click', () => {
        document.getElementById('tpToggle').click();
    });

    document.getElementById('stopLossToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('sl-overlay').classList.toggle('d-none', checked);
        updateStrategyField('stop_loss_enabled', checked);
    });

    document.getElementById('btn-enable-sl').addEventListener('click', () => {
        document.getElementById('stopLossToggle').click();
    });

    document.getElementById('useExistingToggle').addEventListener('change', (e) => {
        updateStrategyField('use_existing', e.target.checked);
    });

    document.getElementById('trailingBuyToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('trailing-buy-settings').classList.toggle('d-none', !checked);
        updateStrategyField('trailing_buy_enabled', checked);
    });

    document.getElementById('consolidatedReentryToggle').addEventListener('change', (e) => {
        updateStrategyField('consolidated_reentry', e.target.checked);
    });

    document.getElementById('trailingBuyDeviation').addEventListener('input', (e) => {
        updateStrategyField('trailing_buy_deviation', parseFloat(e.target.value));
    });
    // Trailing TP Slider
    const trailingSlider = document.getElementById('trailingDeviation');
    if (trailingSlider) {
        trailingSlider.addEventListener('input', (e) => {
            const val = e.target.value;
            const label = document.getElementById('trailing-deviation-label');
            if (label) label.innerText = `${val}%`;
            updateStrategyField('trailing_deviation', parseFloat(val));
        });
    }



    // Buy Type Tabs
    document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            updateOrderTypeUI(tab.dataset.type);
            updateStrategyField('entry_type', tab.dataset.type);
        });
    });

    // Cond Type Tabs
    document.querySelectorAll('#cond-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#cond-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            updateOrderTypeUI(tab.dataset.type);
            updateStrategyField('entry_type', tab.dataset.type);
        });
    });

    // Take Profit Type Tabs
    document.querySelectorAll('#tp-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#tp-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            const isMarket = tab.dataset.type === 'MARKET';
            const ui = (allTranslations[currentLang] || {}).ui || {};
            if (document.getElementById('label-tp_description')) {
                document.getElementById('label-tp_description').innerText = isMarket ? (ui.desc_market || 'Market mode enabled') : (ui.desc_limit || 'Limit mode enabled');
            }
            updateStrategyField('tp_market_mode', isMarket);
        });
    });

    if (document.getElementById('trailingTpToggle')) {
        document.getElementById('trailingTpToggle').addEventListener('change', (e) => {
            const checked = e.target.checked;
            if (document.getElementById('trailing-params')) {
                document.getElementById('trailing-params').classList.toggle('d-none', !checked);
            }
            updateStrategyField('trailing_tp_enabled', checked);
        });
    }


    document.getElementById('bid-price').addEventListener('click', (e) => {
        const val = parseFloat(e.target.innerText);
        if (!isNaN(val)) {
            document.getElementById('inputEntryPrice').value = val;
            updateStrategyField('entry_price', val);
            updateTotalBaseUnits();
        }
    });

    document.getElementById('ask-price').addEventListener('click', (e) => {
        const val = parseFloat(e.target.innerText);
        if (!isNaN(val)) {
            document.getElementById('inputEntryPrice').value = val;
            updateStrategyField('entry_price', val);
            updateTotalBaseUnits();
        }
    });

    // Stop Loss Type Tabs
    document.querySelectorAll('#sl-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#sl-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            const type = tab.dataset.type;
            const ui = (allTranslations[currentLang] || {}).ui || {};

            const group = document.getElementById('sl-order-price-group');
            if (group) group.classList.toggle('d-none', type === 'COND_MARKET');

            const desc = document.getElementById('label-sl_description');
            if (desc) {
                desc.innerText = type === 'COND_MARKET' ?
                    (ui.desc_sl_market || 'The order will be executed at market price when triggered') :
                    (ui.desc_sl_limit || 'The order will be placed on the exchange order book when the price meets Stop Loss conditions');
            }
            updateStrategyField('sl_type', type);
        });
    });

    document.getElementById('stopLossPrice').addEventListener('input', (e) => {
        const val = parseFloat(e.target.value);
        updateStrategyField('stop_loss_price', val);
        updateInternalPct('sl', val);
        // Sync Order Price
        const orderPriceInput = document.getElementById('slOrderPrice');
        if (orderPriceInput) {
            orderPriceInput.value = e.target.value;
            updateStrategyField('sl_order_price', val);
            updateInternalPct('sl-order', val);
        }
    });

    if (document.getElementById('slOrderPrice')) {
        document.getElementById('slOrderPrice').addEventListener('input', (e) => {
            const val = parseFloat(e.target.value);
            updateStrategyField('sl_order_price', val);
            updateInternalPct('sl-order', val);
        });
    }

    // Timeout Adjustment
    const timeoutInput = document.getElementById('slTimeoutDuration');
    if (document.getElementById('slTimeoutMinus')) {
        document.getElementById('slTimeoutMinus').addEventListener('click', () => {
            timeoutInput.value = Math.max(0, parseInt(timeoutInput.value) - 10);
            updateStrategyField('sl_timeout_duration', parseInt(timeoutInput.value));
        });
    }
    if (document.getElementById('slTimeoutPlus')) {
        document.getElementById('slTimeoutPlus').addEventListener('click', () => {
            timeoutInput.value = parseInt(timeoutInput.value) + 10;
            updateStrategyField('sl_timeout_duration', parseInt(timeoutInput.value));
        });
    }

    document.getElementById('slTimeoutToggle').addEventListener('change', (e) => {
        const controls = document.getElementById('sl-timeout-controls');
        if (controls) controls.classList.toggle('d-none', !e.target.checked);
        updateStrategyField('sl_timeout_enabled', e.target.checked);
    });


    document.getElementById('trailingSlToggle').addEventListener('change', (e) => {
        updateStrategyField('trailing_sl_enabled', e.target.checked);
    });

    document.getElementById('moveToBreakevenToggle').addEventListener('change', (e) => {
        updateStrategyField('move_to_breakeven', e.target.checked);
    });

    document.getElementById('trailingTpToggle').addEventListener('change', (e) => {
        updateStrategyField('trailing_tp_enabled', e.target.checked);
    });

    // Settings Modal Toggle
    document.getElementById('configBtn').addEventListener('click', () => {
        populateSettingsModal();
        new bootstrap.Modal(document.getElementById('settingsModal')).show();
    });

    document.getElementById('saveSettingsBtn').addEventListener('click', async () => {
        await saveSettingsFromModal();
        bootstrap.Modal.getInstance(document.getElementById('settingsModal')).hide();
        loadConfig(); // Refresh after save
    });


    // Custom Bottom Tabs
    document.querySelectorAll('[data-tab-target]').forEach(tab => {
        tab.addEventListener('click', () => {
            const target = tab.dataset.tabTarget;
            document.querySelectorAll('.tab-pane-custom').forEach(p => p.classList.add('d-none'));
            document.querySelector(target).classList.remove('d-none');
            document.querySelectorAll('[data-tab-target]').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
        });
    });
}

function updateInternalPct(type, val) {
    if (!activeSymbol || isNaN(val)) return;
    const entry = parseFloat(document.getElementById('inputEntryPrice').value) || parseFloat(document.getElementById('bid-price').innerText);
    if (!entry) return;
    const diff = (val - entry) / entry * 100;
    const badge = document.getElementById(`${type}-pct-badge`);
    badge.innerText = `${diff > 0 ? '+' : ''}${diff.toFixed(2)}%`;
}

function updateTotalBaseUnits() {
    if (!activeSymbol || !currentConfig) return;
    const amountVal = parseFloat(document.getElementById('inputTradeAmountUSDC').value || 0);
    const priceText = document.getElementById('bid-price').innerText;
    const currentPrice = parseFloat(priceText) || 1;
    const entryInput = parseFloat(document.getElementById('inputEntryPrice').value);
    const leverage = (currentConfig.symbol_strategies[activeSymbol] || {}).leverage || 20;
    const isPct = document.getElementById('selectTradeAmountMode').value === 'pct';

    let totalUSDC = amountVal;
    if (isPct) {
        // If %, show estimate based on current dashboard total equity for UI preview
        totalUSDC = (currentBalance * (amountVal / 100.0));
    }

    // The user wants to see Total Notional in USDC (Margin * Leverage)
    const totalNotionalUSDC = totalUSDC * leverage;
    document.getElementById('inputTotalUnits').value = totalNotionalUSDC.toFixed(2);

    // Still calculate units for min requirements check
    const totalUnits = totalNotionalUSDC / (entryInput || currentPrice);
    checkMinRequirements(totalUnits);
}

function checkMinRequirements(units) {
    const minBTC = 0.00015;
    const isBelow = activeSymbol.includes('BTC') && units < minBTC;
    const warning = document.getElementById('min-req-warning');
    if (warning) warning.classList.toggle('d-none', !isBelow);
}

function updateStrategyField(field, value) {
    if (currentConfig && activeSymbol) {
        if (!currentConfig.symbol_strategies[activeSymbol]) currentConfig.symbol_strategies[activeSymbol] = {};
        currentConfig.symbol_strategies[activeSymbol][field] = value;
        // Auto-save on discrete changes
        saveLiveConfig();
    }
}

async function loadConfig() {
    const res = await fetch('/api/config');
    currentConfig = await res.json();
    initSymbolPicker();
    currentLang = currentConfig.language || 'en-US';
    document.getElementById('lang-select').value = currentLang;
    applyUiTranslations();
    updateUIFromConfig();
}

function applyUiTranslations() {
    const ui = (allTranslations[currentLang] || {}).ui || {};
    for (const [key, text] of Object.entries(ui)) {
        const el = document.getElementById(`label-${key}`);
        if (el) el.innerText = text;

        // Custom handling for placeholders
        if (key === 'settings_add_symbol_placeholder') {
            const elInp = document.getElementById('newSymbolInput');
            if (elInp) elInp.placeholder = text;
        }
    }
}

function updateUIFromConfig() {
    if (!activeSymbol || !currentConfig) return;
    const strat = currentConfig.symbol_strategies[activeSymbol] || {};

    // Update active symbol picker to match
    const picker = document.getElementById('selectActiveSymbol');
    if (picker && picker.value !== activeSymbol) {
        picker.value = activeSymbol;
    }

    const isPct = strat.trade_amount_is_pct || false;
    document.getElementById('selectTradeAmountMode').value = isPct ? 'pct' : 'fixed';
    document.getElementById('inputTradeAmountUSDC').value = strat.trade_amount_usdc || 100;
    document.getElementById('asset-symbol-label').classList.toggle('d-none', isPct);
    document.getElementById('pct-symbol-label').classList.toggle('d-none', !isPct);

    document.getElementById('inputEntryPrice').value = strat.entry_price || 0;

    const entryType = strat.entry_type || 'LIMIT';
    updateOrderTypeUI(entryType);

    const tpOn = strat.tp_enabled !== false;
    document.getElementById('tpToggle').checked = tpOn;
    // Explicitly toggle overlay visibility
    const tpOverlay = document.getElementById('tp-overlay');
    if (tpOn) tpOverlay.classList.add('d-none');
    else tpOverlay.classList.remove('d-none');
    const tpMarket = strat.tp_market_mode || false;
    document.querySelectorAll('#tp-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', (t.dataset.type === 'MARKET') === tpMarket));

    if (typeof renderTpTargets === 'function') renderTpTargets();

    const slOn = strat.stop_loss_enabled || false;
    document.getElementById('stopLossToggle').checked = slOn;

    const trailingTpOn = strat.trailing_tp_enabled || false;
    document.getElementById('trailingTpToggle').checked = trailingTpOn;
    document.getElementById('trailing-params').classList.toggle('d-none', !trailingTpOn);
    document.getElementById('trailingDeviation').value = strat.trailing_deviation || 0.5;
    document.getElementById('trailing-deviation-label').innerText = `${strat.trailing_deviation || 0.5}%`;

    const slPriceEl = document.getElementById('stopLossPrice');
    if (slPriceEl) {
        slPriceEl.value = strat.stop_loss_price || 0;
        updateInternalPct('sl', strat.stop_loss_price || 0);
    }

    const slTimeoutToggle = document.getElementById('slTimeoutToggle');
    if (slTimeoutToggle) slTimeoutToggle.checked = strat.sl_timeout_enabled || false;

    const slTimeoutControls = document.getElementById('sl-timeout-controls');
    if (slTimeoutControls) slTimeoutControls.classList.toggle('d-none', !strat.sl_timeout_enabled);

    const slTimeoutDuration = document.getElementById('slTimeoutDuration');
    if (slTimeoutDuration) slTimeoutDuration.value = strat.sl_timeout_duration || 300;

    const slType = strat.sl_type || 'COND_LIMIT';
    document.querySelectorAll('#sl-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === slType));

    const slOrderPriceGroup = document.getElementById('sl-order-price-group');
    if (slOrderPriceGroup) slOrderPriceGroup.classList.toggle('d-none', slType === 'COND_MARKET');

    if (strat.sl_trigger_source) {
        const btn = document.getElementById('slTriggerSourceBtn');
        if (btn) btn.innerText = strat.sl_trigger_source;
    }

    const slOrderPrice = strat.sl_order_price || strat.stop_loss_price || 0;
    const slOrderPriceInput = document.getElementById('slOrderPrice');
    if (slOrderPriceInput) {
        slOrderPriceInput.value = slOrderPrice;
        updateInternalPct('sl-order', slOrderPrice);
    }

    const trailingSlToggle = document.getElementById('trailingSlToggle');
    if (trailingSlToggle) trailingSlToggle.checked = strat.trailing_sl_enabled || false;

    const moveToBreakevenToggle = document.getElementById('moveToBreakevenToggle');
    if (moveToBreakevenToggle) moveToBreakevenToggle.checked = strat.move_to_breakeven || false;

    const useExistingToggle = document.getElementById('useExistingToggle');
    if (useExistingToggle) useExistingToggle.checked = strat.use_existing || false;

    const consolidatedOn = strat.consolidated_reentry || false;
    document.getElementById('consolidatedReentryToggle').checked = consolidatedOn;

    const trailingBuyToggle = document.getElementById('trailingBuyToggle');
    if (trailingBuyToggle) trailingBuyToggle.checked = strat.trailing_buy_enabled || false;

    const tbSettings = document.getElementById('trailing-buy-settings');
    if (tbSettings) tbSettings.classList.toggle('d-none', !strat.trailing_buy_enabled);

    const tbd = document.getElementById('trailingBuyDeviation');
    if (tbd) tbd.value = strat.trailing_buy_deviation || 0.1;

    if (typeof renderTpTargets === 'function') renderTpTargets();
    updateTotalBaseUnits();
}


function updateOrderTypeUI(type) {
    const descEl = document.getElementById('order-type-desc');
    const condOptions = document.getElementById('cond-options');
    const priceInputGroup = document.getElementById('inputEntryPrice').closest('.mb-2');
    const triggerLabel = document.getElementById('label-trigger_price_field');
    const ui = (allTranslations[currentLang] || {}).ui || {};

    // Default visibility
    condOptions.classList.add('d-none');
    priceInputGroup.classList.remove('d-none');

    if (['LIMIT', 'MARKET', 'CONDITIONAL'].includes(type)) {
        document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === type));
    } else {
        document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === 'CONDITIONAL'));
        condOptions.classList.remove('d-none');
        document.querySelectorAll('#cond-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === type));
    }

    switch (type) {
        case 'LIMIT': descEl.innerText = ui.desc_limit; break;
        case 'MARKET':
            descEl.innerText = ui.desc_market;
            priceInputGroup.classList.add('d-none');
            break;
        case 'CONDITIONAL':
            descEl.innerText = ui.desc_conditional;
            condOptions.classList.remove('d-none');
            break;
        case 'COND_LIMIT':
            descEl.innerText = ui.desc_cond_limit;
            condOptions.classList.remove('d-none');
            break;
        case 'COND_MARKET':
            descEl.innerText = ui.desc_cond_market;
            condOptions.classList.remove('d-none');
            priceInputGroup.classList.add('d-none');
            break;
    }
}

function initSymbolPicker() {
    const select = document.getElementById('selectActiveSymbol');
    select.innerHTML = currentConfig.symbols.map(s => `<option value="${s}">${s}</option>`).join('');
    if (currentConfig.symbols.length > 0) {
        activeSymbol = currentConfig.symbols[0];
        const unit = 'USDC';
        const labels = ['asset-symbol-label', 'base-asset-label', 'entry-price-asset-label', 'tp-price-asset-label', 'sl-price-asset-label', 'sl-order-price-asset-label'];
        labels.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerText = unit;
        });
    }
}





function setupSocketListeners() {
    socket.on('bot_status', (data) => {
        isBotRunning = data.running;
        const btn = document.getElementById('startStopBtn');
        const spinner = document.getElementById('startStopSpinner');
        const label = document.getElementById('label-start') || document.getElementById('label-stop');

        btn.disabled = false;
        if (spinner) spinner.classList.add('d-none');

        const ui = (allTranslations[currentLang] || {}).ui || {};
        const text = isBotRunning ? (ui.stop || 'Stop') : (ui.start || 'Start');

        if (label) {
            label.innerText = text;
            label.id = isBotRunning ? 'label-stop' : 'label-start';
        } else {
            btn.innerText = text; // Fallback
        }

        btn.className = isBotRunning ? 'btn btn-sm btn-danger px-4 d-flex align-items-center gap-2' : 'btn btn-sm btn-accent px-4 d-flex align-items-center gap-2';
    });

    socket.on('price_update', (prices) => {
        if (activeSymbol && prices[activeSymbol]) {
            const p = prices[activeSymbol];
            const bid = p.bid !== undefined ? p.bid : p.last || p;
            const ask = p.ask !== undefined ? p.ask : p.last || p;

            const bidEl = document.getElementById('bid-price');
            const askEl = document.getElementById('ask-price');

            // Determine precision based on price magnitude
            // For DOGE (~0.15) we need at least 4-5 decimals
            // For BTC (~70000) 1-2 decimals is enough
            let precision = 2;
            if (bid < 0.1) precision = 6;
            else if (bid < 1) precision = 5;
            else if (bid < 10) precision = 4;
            else if (bid < 100) precision = 3;

            if (bidEl && typeof bid === 'number') bidEl.innerText = bid.toFixed(precision);
            if (askEl && typeof ask === 'number') askEl.innerText = ask.toFixed(precision);

            // Auto-populate entry price if it's 0 and we JUST received a fresh market price
            if (currentConfig && currentConfig.symbol_strategies[activeSymbol]) {
                const strat = currentConfig.symbol_strategies[activeSymbol];
                if (!strat.entry_price || strat.entry_price === 0) {
                    strat.entry_price = bid;
                    document.getElementById('inputEntryPrice').value = bid.toFixed(precision);
                    saveLiveConfig();
                }
            }

            updateTotalBaseUnits();
        }
    });

    socket.on('clear_console', () => {
        const out = document.getElementById('consoleOutput');
        if (out) out.innerHTML = '';
    });

    socket.on('account_update', (data) => {
        const container = document.getElementById('individual-accounts-container');
        container.innerHTML = (data.accounts || []).map(acc => `
            <div class="account-card ${acc.has_client ? '' : 'opacity-50'}" style="min-width: 150px">
                <div class="d-flex justify-content-between align-items-center mb-1">
                    <span class="small fw-bold text-secondary text-uppercase">${acc.name}</span>
                    <div class="account-dot ${acc.active ? 'bg-success' : 'bg-secondary'}" style="width:6px; height:6px; border-radius:50%"></div>
                </div>
                <span class="text-primary fw-bold">${acc.has_client && acc.balance !== undefined ? '$' + acc.balance.toFixed(2) : 'Disconnected'}</span>
            </div>
        `).join('');

        currentBalance = data.total_equity || 0;
        const totalEquityVal = document.getElementById('total-equity-val');
        if (totalEquityVal) totalEquityVal.innerText = `$${currentBalance.toFixed(2)}`;

        const posTable = document.getElementById('positionsTableBody');
        posTable.innerHTML = (data.positions || []).map(p => `
            <tr class="border-0">
                <td>${p.account}</td>
                <td>${p.symbol}</td>
                <td class="${p.amount > 0 ? 'text-success' : 'text-danger'}">${p.amount}</td>
                <td>${(parseFloat(p.entryPrice) || 0).toFixed(2)}</td>
                <td class="${p.unrealizedProfit >= 0 ? 'text-success' : 'text-danger'}">${(parseFloat(p.unrealizedProfit) || 0).toFixed(2)}</td>
                <td><button class="btn btn-xs btn-outline-danger py-0" onclick="closePosition(${p.account_idx}, '${p.symbol}')">Kill</button></td>
            </tr>
        `).join('');
    });

    socket.on('console_log', (data) => {
        const out = document.getElementById('consoleOutput');
        const div = document.createElement('div');
        div.className = `small mb-1 console-entry ${data.level === 'error' ? 'text-danger' : 'text-success'}`;
        // data.rendered is pre-rendered on backend but if we changed language
        // we might get the whole history or just updates.
        div.innerText = `[${data.timestamp}] ${data.rendered || data.message}`;
        out.appendChild(div);
        out.scrollTop = out.scrollHeight;
    });
}

function populateSettingsModal() {
    if (!currentConfig) return;
    document.getElementById('demoModeToggle').checked = currentConfig.is_demo || false;

    // Global Settings (from first symbol or defaults)
    const firstSym = currentConfig.symbols[0];
    const currentLev = firstSym ? (currentConfig.symbol_strategies[firstSym]?.leverage || 20) : 20;
    const currentDir = firstSym ? (currentConfig.symbol_strategies[firstSym]?.direction || 'LONG') : 'LONG';
    const currentMargin = firstSym ? (currentConfig.symbol_strategies[firstSym]?.margin_type || 'CROSSED') : 'CROSSED';

    document.getElementById('globalLeverageInput').value = currentLev;
    document.getElementById('globalDirectionSelect').value = currentDir;
    document.getElementById('globalMarginModeSelect').value = currentMargin;

    // Accounts
    const accContainer = document.getElementById('api-accounts-settings');
    const ui = (allTranslations[currentLang] || {}).ui || {};

    accContainer.innerHTML = (currentConfig.api_accounts || []).map((acc, i) => `
        <div class="row g-2 mb-2 align-items-center account-setting-row" data-idx="${i}">
            <div class="col-2"><input type="text" class="form-control form-control-sm bg-dark text-light border-secondary" placeholder="${ui.acc_name_placeholder || 'Name'}" value="${acc.name || ''}" id="acc-name-${i}"></div>
            <div class="col-3"><input type="text" class="form-control form-control-sm bg-dark text-light border-secondary" placeholder="${ui.acc_key_placeholder || 'Key'}" value="${acc.api_key || ''}" id="acc-key-${i}"></div>
            <div class="col-3"><input type="password" class="form-control form-control-sm bg-dark text-light border-secondary" placeholder="${ui.acc_secret_placeholder || 'Secret'}" value="${acc.api_secret || ''}" id="acc-secret-${i}"></div>
            <div class="col-2 text-center">
                <button class="btn btn-xs btn-outline-info" onclick="testApiKey(${i})" id="test-btn-${i}">${ui.settings_test_btn || 'Test'}</button>
            </div>
            <div class="col-2 text-end">
                <div class="form-check form-switch d-inline-block">
                    <input class="form-check-input" type="checkbox" id="acc-enabled-${i}" ${acc.enabled !== false ? 'checked' : ''}>
                </div>
            </div>
        </div>
    `).join('');

    renderSymbolsInModal();
}

function renderSymbolsInModal() {
    const list = document.getElementById('symbols-list');
    list.innerHTML = currentConfig.symbols.map(s => `
        <div class="badge bg-secondary d-flex align-items-center gap-2 p-2">
            ${s}
            <i class="bi bi-x-circle cursor-pointer text-danger" onclick="removeSymbol('${s}')"></i>
        </div>
    `).join('');
}

window.removeSymbol = (sym) => {
    currentConfig.symbols = currentConfig.symbols.filter(s => s !== sym);
    renderSymbolsInModal();
};

async function saveSettingsFromModal() {
    const isDemo = document.getElementById('demoModeToggle').checked;

    // Accounts
    const api_accounts = [];
    document.querySelectorAll('.account-setting-row').forEach(row => {
        const i = row.dataset.idx;
        api_accounts.push({
            name: document.getElementById(`acc-name-${i}`).value,
            api_key: document.getElementById(`acc-key-${i}`).value,
            api_secret: document.getElementById(`acc-secret-${i}`).value,
            enabled: document.getElementById(`acc-enabled-${i}`).checked
        });
    });

    currentConfig.is_demo = isDemo;
    currentConfig.api_accounts = api_accounts;

    // Apply Global Settings to all symbols
    const globalLeverage = parseInt(document.getElementById('globalLeverageInput').value) || 20;
    const globalDirection = document.getElementById('globalDirectionSelect').value;
    const globalMarginMode = document.getElementById('globalMarginModeSelect').value;

    for (const sym in currentConfig.symbol_strategies) {
        currentConfig.symbol_strategies[sym].leverage = globalLeverage;
        currentConfig.symbol_strategies[sym].direction = globalDirection;
        currentConfig.symbol_strategies[sym].margin_type = globalMarginMode;
    }

    await saveLiveConfig();
}

async function saveLiveConfig(extra = {}) {
    if (!currentConfig) return;
    const payload = { ...currentConfig, ...extra };
    await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
}

window.testApiKey = async (i) => {
    const key = document.getElementById(`acc-key-${i}`).value;
    const secret = document.getElementById(`acc-secret-${i}`).value;
    const isDemo = document.getElementById('demoModeToggle').checked;
    const btn = document.getElementById(`test-btn-${i}`);

    if (!key || !secret) {
        alert("Please enter API key and secret");
        return;
    }

    btn.disabled = true;
    const oldText = btn.innerText;
    btn.innerText = "...";

    try {
        const res = await fetch('/api/test_api_key', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ api_key: key, api_secret: secret, is_demo: isDemo })
        });
        const data = await res.json();
        alert(data.message);
    } catch (e) {
        alert("Error testing API key: " + e);
    } finally {
        btn.disabled = false;
        btn.innerText = oldText;
    }
};

window.closePosition = (account_idx, symbol) => { socket.emit('close_trade', { account_idx, symbol }); };

// TP Split Management
function renderTpTargets() {
    if (!activeSymbol || !currentConfig) return;
    const strat = currentConfig.symbol_strategies[activeSymbol] || {};
    const targets = strat.tp_targets || [];
    const container = document.getElementById('tp-targets-list');
    const entryPrice = parseFloat(document.getElementById('inputEntryPrice').value) || 0;
    const unit = 'USDC';
    const direction = strat.direction || 'LONG';

    container.innerHTML = targets.map((target, i) => {
        const pct = parseFloat(target.percent) || 0;
        const price = direction === 'LONG' ? entryPrice * (1 + pct / 100) : entryPrice * (1 - pct / 100);
        return `
            <div class="tp-target-row mb-3 p-3 rounded bg-dark-accent border border-secondary shadow-sm" data-idx="${i}">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <label class="small text-secondary fw-bold">Target ${i + 1}</label>
                    <button class="btn btn-link btn-sm p-0 text-danger border-0" onclick="removeTpTarget(${i})">
                        <i class="bi bi-trash3"></i>
                    </button>
                </div>
                <div class="input-group input-group-sm mb-3">
                    <input type="number" class="form-control bg-dark border-secondary text-light fw-bold" 
                        value="${target.percent}" step="0.1" 
                        onchange="updateTpTarget(${i}, 'percent', this.value)">
                    <span class="input-group-text bg-dark border-secondary text-secondary">%</span>
                    <span class="input-group-text bg-transparent border-0 text-accent fw-bold ms-auto">${price.toFixed(5)} ${unit}</span>
                </div>
                <div class="small d-flex justify-content-between text-secondary mb-1">
                    <span>Volume</span>
                    <span class="fw-bold text-light">${target.volume}%</span>
                </div>
                <input type="range" class="form-range custom-range" min="1" max="100" 
                    value="${target.volume}" 
                    oninput="updateTpTarget(${i}, 'volume', this.value)">
            </div>
        `;
    }).join('');
}

window.addTpTarget = () => {
    if (!activeSymbol) return;
    if (!currentConfig.symbol_strategies[activeSymbol]) currentConfig.symbol_strategies[activeSymbol] = {};
    const strat = currentConfig.symbol_strategies[activeSymbol];
    if (!strat.tp_targets) strat.tp_targets = [];

    // Default: split volume evenly among targets if possible, or just add 25%
    const lastPct = strat.tp_targets.length > 0 ? parseFloat(strat.tp_targets[strat.tp_targets.length - 1].percent) : 0;
    strat.tp_targets.push({ percent: (lastPct + 1).toFixed(2), volume: 25 });
    renderTpTargets();
    saveLiveConfig();
};

window.removeTpTarget = (idx) => {
    const strat = currentConfig.symbol_strategies[activeSymbol];
    strat.tp_targets.splice(idx, 1);
    renderTpTargets();
    saveLiveConfig();
};

window.updateTpTarget = (idx, field, value) => {
    const strat = currentConfig.symbol_strategies[activeSymbol];
    strat.tp_targets[idx][field] = value;
    renderTpTargets();
    saveLiveConfig();
};

document.getElementById('addTpTargetBtn').addEventListener('click', () => {
    window.addTpTarget();
});

window.setSlTriggerSource = (source) => {
    document.getElementById('slTriggerSourceBtn').innerText = source;
    updateStrategyField('sl_trigger_source', source);
};


