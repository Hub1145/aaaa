import unittest
from bot_engine import BinanceTradingBotEngine
from decimal import Decimal

class TestFormatting(unittest.TestCase):
    def setUp(self):
        # Mock engine
        self.bot = BinanceTradingBotEngine('config.json', lambda x, y: None)
        self.bot.shared_market_data['BTCUSDC'] = {
            'info': {
                'filters': [
                    {'filterType': 'LOT_SIZE', 'stepSize': '0.00001'},
                    {'filterType': 'PRICE_FILTER', 'tickSize': '0.01'}
                ]
            }
        }
        self.bot.shared_market_data['DOGEUSDC'] = {
            'info': {
                'filters': [
                    {'filterType': 'LOT_SIZE', 'stepSize': '1'},
                    {'filterType': 'PRICE_FILTER', 'tickSize': '0.00001'}
                ]
            }
        }

    def test_quantity_formatting(self):
        # BTC: 0.00001 step
        self.assertEqual(self.bot._format_quantity('BTCUSDC', 0.123456), '0.12345')
        self.assertEqual(self.bot._format_quantity('BTCUSDC', 1.0), '1.00000')

        # DOGE: 1 step
        self.assertEqual(self.bot._format_quantity('DOGEUSDC', 123.456), '123')
        self.assertEqual(self.bot._format_quantity('DOGEUSDC', 50.0), '50')

    def test_price_formatting(self):
        # BTC: 0.01 tick
        self.assertEqual(self.bot._format_price('BTCUSDC', 60000.1234), '60000.12')
        self.assertEqual(self.bot._format_price('BTCUSDC', 60000), '60000.00')

        # DOGE: 0.00001 tick
        self.assertEqual(self.bot._format_price('DOGEUSDC', 0.1234567), '0.12346')
        self.assertEqual(self.bot._format_price('DOGEUSDC', 0.12), '0.12000')

if __name__ == '__main__':
    unittest.main()
