from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import json
import os
import threading
from translations_py import TRANSLATIONS

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mock-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

config_file = 'config.json'

def load_config():
    with open(config_file, 'r') as f:
        return json.load(f)

@app.route('/')
def index():
    return render_template('dashboard.html', translations=TRANSLATIONS, server_ip="1.2.3.4")

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify(load_config())

@app.route('/api/config', methods=['POST'])
def update_config():
    return jsonify({'success': True})

@socketio.on('connect')
def handle_connect():
    config = load_config()
    emit('bot_status', {'running': False})

    # Mock account update
    accounts = []
    for acc in config.get('api_accounts', []):
        accounts.append({
            'name': acc.get('name'),
            'balance': 1000.0,
            'active': False,
            'has_client': True,
            'error': None,
            'last_update': 0
        })

    emit('account_update', {
        'total_balance': 4000.0,
        'total_equity': 4000.0,
        'total_pnl': 0.0,
        'positions': [],
        'accounts': accounts
    })

    # Mock price update
    prices = {}
    for s in config.get('symbols', []):
        prices[s] = {'bid': 50000.0, 'ask': 50001.0, 'last': 50000.5}
    emit('price_update', prices)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=3000, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)
