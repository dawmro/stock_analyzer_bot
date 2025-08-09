from django.shortcuts import render
from django.http import JsonResponse
from django.utils import timezone
from datetime import timedelta
from django.db.models import Min, Max, Sum
from django.db.models.functions import TruncDate
from .models import Company, StockQuote, StockIndicator

def stock_chart_view(request):
    """
    View to render stock chart page
    """
    return render(request, 'market/stock_chart.html')

def stock_data_api(request, ticker="X:BTCUSD"):
    """
    API endpoint to get daily aggregated stock data for visualization
    Default ticker: X:BTCUSD
    Returns JSON with:
        - dates: list of dates (YYYY-MM-DD)
        - daily_data: list of {open, high, low, close} objects
        - volumes: list of daily volumes
        - scores: list of indicator scores
        - indicators: dict of indicator values (ma_5, ma_20, rsi)
    """
    # Get company object
    try:
        company = Company.objects.get(ticker=ticker)
    except Company.DoesNotExist:
        return JsonResponse({"error": "Company not found"}, status=404)
    
    # Calculate date range (last 30 days)
    end_date = timezone.now()
    start_date = end_date - timedelta(days=30)
    
    # Get daily aggregated stock quotes
    daily_quotes = (
        StockQuote.objects
        .filter(company=company, time__range=(start_date, end_date))
        .annotate(date=TruncDate('time'))
        .values('date')
        .annotate(
            open=Min('open_price'),
            high=Max('high_price'),
            low=Min('low_price'),
            close=Max('close_price'),
            volume=Sum('volume')
        )
        .order_by('date')
    )
    
    # Get daily indicators
    indicators = (
        StockIndicator.objects
        .filter(company=company, time__range=(start_date, end_date))
        .annotate(date=TruncDate('time'))
        .order_by('date')
    )
    
    # Prepare data structure
    data = {
        "dates": [],
        "daily_data": [],
        "volumes": [],
        "scores": [],
        "indicators": {
            "ma_5": [],
            "ma_20": [],
            "rsi": []
        }
    }
    
    # Process daily quotes
    for quote in daily_quotes:
        data["dates"].append(quote['date'].strftime("%Y-%m-%d"))
        data["daily_data"].append({
            "open": float(quote['open']),
            "high": float(quote['high']),
            "low": float(quote['low']),
            "close": float(quote['close'])
        })
        data["volumes"].append(float(quote['volume']))
    
    # Process indicators
    for indicator in indicators:
        data["scores"].append(float(indicator.score))
        ind_data = indicator.indicators
        data["indicators"]["ma_5"].append(ind_data.get("ma_5"))
        data["indicators"]["ma_20"].append(ind_data.get("ma_20"))
        data["indicators"]["rsi"].append(ind_data.get("rsi"))
    
    return JsonResponse(data)
