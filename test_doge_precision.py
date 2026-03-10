import unittest
from bot_engine import BinanceTradingBotEngine
from unittest.mock import MagicMock, patch

class TestDogePrecision(unittest.TestCase):
    @patch('bot_engine.Client')
    def setUp(self, mock_client):
        self.bot = BinanceTradingBotEngine('config.json', lambda x, y: None)
        self.bot.shared_market_data['DOGEUSDC'] = {
            'info': {
                'filters': [
                    {'filterType': 'LOT_SIZE', 'stepSize': '1'},
                    {'filterType': 'PRICE_FILTER', 'tickSize': '0.00001'}
                ]
            }
        }

    def test_doge_formatting(self):
        # Quantity should be whole numbers
        self.assertEqual(self.bot._format_quantity('DOGEUSDC', 1234.56), '1234')

        # Price should have 5 decimals
        self.assertEqual(self.bot._format_price('DOGEUSDC', 0.123456), '0.12346')
        self.assertEqual(self.bot._format_price('DOGEUSDC', 0.1), '0.10000')

if __name__ == '__main__':
    unittest.main()
