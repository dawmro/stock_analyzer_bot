from django.test import TestCase, RequestFactory
from django.utils import timezone
from datetime import timedelta, datetime
from .models import Company, StockQuote, StockIndicator
from .views import stock_data_api
import json

class StockDataAPITestCase(TestCase):
    def setUp(self):
        # Create test company
        self.company = Company.objects.create(
            ticker="TEST",
            name="Test Company",
            active=True
        )
        
        # Create dates for testing
        today = timezone.now().date()
        self.dates = [today - timedelta(days=i) for i in range(5, 0, -1)]
        
        # Create stock quotes with all required fields
        for i, date in enumerate(self.dates):
            # Create timezone-aware datetime at start of day
            aware_time = timezone.make_aware(datetime.combine(date, datetime.min.time()))
            
            StockQuote.objects.create(
                company=self.company,
                time=aware_time,
                open_price=100 + i,
                high_price=110 + i,
                low_price=90 + i,
                close_price=105 + i,
                volume=1000 * (i+1),
                volume_weighted_average=102.5 + i,  # Required field
                number_of_trades=500 * (i+1)        # Optional but good to include
            )
        
        # Create indicators (missing for date index 2) with timezone-aware datetimes
        indicator_dates = [d for i, d in enumerate(self.dates) if i != 2]
        for i, date in enumerate(indicator_dates):
            # Create timezone-aware datetime at start of day
            aware_time = timezone.make_aware(datetime.combine(date, datetime.min.time()))
            
            StockIndicator.objects.create(
                company=self.company,
                time=aware_time,
                score=0.5 + (i * 0.1),
                indicators={
                    "ma_5": 105 + i,
                    "ma_20": 102 + i,
                    "rsi": 60 + i
                }
            )
        
        self.factory = RequestFactory()
    
    def test_api_response_structure(self):
        request = self.factory.get('/stock-data/TEST/')
        response = stock_data_api(request, ticker="TEST")
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        # Verify top-level structure
        self.assertIn("dates", data)
        self.assertIn("daily_data", data)
        self.assertIn("volumes", data)
        self.assertIn("scores", data)
        self.assertIn("indicators", data)
        
        # Verify indicators sub-structure
        self.assertIn("ma_5", data["indicators"])
        self.assertIn("ma_20", data["indicators"])
        self.assertIn("rsi", data["indicators"])
    
    def test_data_alignment_with_missing_indicators(self):
        request = self.factory.get('/stock-data/TEST/')
        response = stock_data_api(request, ticker="TEST")
        data = json.loads(response.content)
        
        # Verify all arrays have same length
        num_dates = len(data["dates"])
        self.assertEqual(len(data["daily_data"]), num_dates)
        self.assertEqual(len(data["volumes"]), num_dates)
        self.assertEqual(len(data["scores"]), num_dates)
        self.assertEqual(len(data["indicators"]["ma_5"]), num_dates)
        self.assertEqual(len(data["indicators"]["ma_20"]), num_dates)
        self.assertEqual(len(data["indicators"]["rsi"]), num_dates)
        
        # Verify placeholder for missing indicator (index 2)
        self.assertIsNone(data["scores"][2])
        self.assertIsNone(data["indicators"]["ma_5"][2])
        self.assertIsNone(data["indicators"]["ma_20"][2])
        self.assertIsNone(data["indicators"]["rsi"][2])
        
        # Verify existing indicators
        self.assertAlmostEqual(data["scores"][0], 0.5)
        self.assertAlmostEqual(data["scores"][1], 0.6)
        self.assertAlmostEqual(data["scores"][3], 0.7)
        self.assertAlmostEqual(data["scores"][4], 0.8)
    
    def test_no_indicators(self):
        # Delete all indicators
        StockIndicator.objects.all().delete()
        
        request = self.factory.get('/stock-data/TEST/')
        response = stock_data_api(request, ticker="TEST")
        data = json.loads(response.content)
        
        # Verify all indicator arrays are None placeholders
        self.assertTrue(all(score is None for score in data["scores"]))
        self.assertTrue(all(ma5 is None for ma5 in data["indicators"]["ma_5"]))
        self.assertTrue(all(ma20 is None for ma20 in data["indicators"]["ma_20"]))
        self.assertTrue(all(rsi is None for rsi in data["indicators"]["rsi"]))
    
    def test_no_quotes(self):
        """Test when no quotes exist for the company"""
        # Delete all quotes
        StockQuote.objects.all().delete()
        
        request = self.factory.get('/stock-data/TEST/')
        response = stock_data_api(request, ticker="TEST")
        data = json.loads(response.content)
        
        # Verify all arrays are empty
        self.assertEqual(len(data["dates"]), 0)
        self.assertEqual(len(data["daily_data"]), 0)
        self.assertEqual(len(data["volumes"]), 0)
        self.assertEqual(len(data["scores"]), 0)
        self.assertEqual(len(data["indicators"]["ma_5"]), 0)
        self.assertEqual(len(data["indicators"]["ma_20"]), 0)
        self.assertEqual(len(data["indicators"]["rsi"]), 0)
    
    def test_partial_indicators(self):
        """Test when only some indicators exist"""
        # Delete indicators for the first date
        first_date = self.dates[0]
        StockIndicator.objects.filter(time__date=first_date).delete()
        
        request = self.factory.get('/stock-data/TEST/')
        response = stock_data_api(request, ticker="TEST")
        data = json.loads(response.content)
        
        # Verify first indicator is None
        self.assertIsNone(data["scores"][0])
        self.assertIsNone(data["indicators"]["ma_5"][0])
        self.assertIsNone(data["indicators"]["ma_20"][0])
        self.assertIsNone(data["indicators"]["rsi"][0])
        
        # Verify other indicators exist
        for i in range(1, len(self.dates)):
            if i != 2:  # Skip the missing indicator we set up
                self.assertIsNotNone(data["scores"][i])
                self.assertIsNotNone(data["indicators"]["ma_5"][i])
                self.assertIsNotNone(data["indicators"]["ma_20"][i])
                self.assertIsNotNone(data["indicators"]["rsi"][i])
    
    def test_invalid_ticker(self):
        request = self.factory.get('/stock-data/INVALID/')
        response = stock_data_api(request, ticker="INVALID")
        
        self.assertEqual(response.status_code, 404)
        data = json.loads(response.content)
        self.assertEqual(data["error"], "Company not found")
