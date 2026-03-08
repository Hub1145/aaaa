from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import json
import logging
import os
import threading
import requests
from bot_engine import BinanceTradingBotEngine
from translations_py import TRANSLATIONS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SESSION_SECRET', 'binance-bot-secret')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

config_file = 'config.json'
bot_engine = None
server_ip = "Unknown"

try:
    server_ip = requests.get('https://api.ipify.org', timeout=5).text
except Exception:
    pass

def load_config():
    with open(config_file, 'r') as f:
        return json.load(f)

def save_config(config):
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

def emit_to_client(event, data):
    socketio.emit(event, data)

@app.route('/')
def index():
    return render_template('dashboard.html', translations=TRANSLATIONS, server_ip=server_ip)

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify(load_config())

@app.route('/api/config', methods=['POST'])
def update_config():
    global bot_engine
    try:
        new_config = request.json
        # Basic validation
        required_top_keys = ['api_accounts', 'is_demo', 'symbols', 'symbol_strategies', 'language']
        if not all(k in new_config for k in required_top_keys):
            missing = [k for k in required_top_keys if k not in new_config]
            return jsonify({'success': False, 'message': f'Missing required configuration keys: {missing}'}), 400

        save_config(new_config)
        if bot_engine:
            bot_engine.apply_live_config_update(new_config)
        return jsonify({'success': True, 'message': 'Configuration updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/shutdown', methods=['POST'])
def shutdown():
    global bot_engine
    if bot_engine:
        bot_engine.stop()

    def kill_server():
        import time
        import signal
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=kill_server).start()
    return jsonify({'success': True, 'message': 'Server shutting down...'})

@app.route('/api/download_logs')
def download_logs():
    try:
        log_file = 'binance_bot.log'
        if not os.path.exists(log_file):
             return jsonify({'error': 'Log file not found'}), 404
        return send_file(log_file, mimetype='text/plain', as_attachment=True, download_name='bot_log.log')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/test_api_key', methods=['POST'])
def test_api_key_route():
    try:
        data = request.json
        api_key = data.get('api_key')
        api_secret = data.get('api_secret')
        is_demo = data.get('is_demo', True)
        
        if not api_key or not api_secret:
            return jsonify({'success': False, 'message': 'API Key and Secret are required.'}), 400

        success, msg = BinanceTradingBotEngine.test_account(api_key, api_secret, is_demo=is_demo)
        
        # Translate test result message if possible
        lang = load_config().get('language', 'pt-BR')
        if msg == "Connection successful":
            msg = TRANSLATIONS[lang].get('conn_success', msg)
        elif not api_key or not api_secret:
            msg = TRANSLATIONS[lang].get('api_keys_missing', msg)
            
        return jsonify({'success': success, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    global bot_engine
    if not bot_engine:
        bot_engine = BinanceTradingBotEngine(config_file, emit_to_client)

    emit('bot_status', {'running': bot_engine.is_running})
    emit('clear_console', {})
    for log in list(bot_engine.console_logs):
        emit('console_log', log)
    bot_engine._emit_account_update()
    bot_engine._emit_latest_prices()

@socketio.on('start_bot')
def handle_start_bot():
    global bot_engine
    if not bot_engine:
        bot_engine = BinanceTradingBotEngine(config_file, emit_to_client)

    if not bot_engine.is_running:
        bot_engine.start()
        emit('bot_status', {'running': True}, broadcast=True)
        emit('success', {'message': 'Bot started successfully'})

@socketio.on('stop_bot')
def handle_stop_bot():
    global bot_engine
    if bot_engine and bot_engine.is_running:
        bot_engine.stop()
        emit('bot_status', {'running': False}, broadcast=True)
        emit('success', {'message': 'Bot stopped successfully'})

@socketio.on('clear_console')
def handle_clear_console():
    if bot_engine:
        bot_engine.console_logs.clear()
    emit('console_cleared', {})

@socketio.on('close_trade')
def handle_close_trade(data):
    if bot_engine:
        account_idx = data.get('account_idx')
        symbol = data.get('symbol')
        if account_idx is not None and symbol:
            bot_engine.close_position(account_idx, symbol)

if __name__ == '__main__':
    if not bot_engine:
        bot_engine = BinanceTradingBotEngine(config_file, emit_to_client)

    port = int(os.environ.get('PORT', 3000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)
