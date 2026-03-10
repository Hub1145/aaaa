import unittest
from unittest.mock import MagicMock, patch
from bot_engine import BinanceTradingBotEngine

class TestLogic(unittest.TestCase):
    @patch('bot_engine.Client')
    def setUp(self, mock_client):
        self.bot = BinanceTradingBotEngine('config.json', lambda x, y: None)
        self.bot.account_balances[0] = 1000.0
        self.bot.config['symbol_strategies'] = {
            'BTCUSDC': {
                'leverage': '10',
                'entry_price': '50000'
            }
        }

    def test_balance_check(self):
        # 1000 balance, 10x leverage
        # 1 BTC @ 50000 = 50000 notional
        # Margin required = 50000 / 10 = 5000
        # 5000 > 1000, should return False
        self.assertFalse(self.bot._check_balance_for_order(0, 1.0, 50000, leverage=10))

        # 0.1 BTC @ 50000 = 5000 notional
        # Margin required = 5000 / 10 = 500
        # 500 < 1000, should return True
        self.assertTrue(self.bot._check_balance_for_order(0, 0.1, 50000, leverage=10))

if __name__ == '__main__':
    unittest.main()
