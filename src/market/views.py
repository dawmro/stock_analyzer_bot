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
    # Get all active companies ordered by ticker
    companies = Company.objects.filter(active=True).order_by('ticker')
    return render(request, 'market/stock_chart.html', {'companies': companies})

def stock_data_api(request, ticker="X:BTCUSD"):
    """
    API endpoint to get stock data for visualization
    Default ticker: X:BTCUSD
    Query parameters:
        days: Number of days to retrieve (default: 30)
    Returns JSON with:
        - dates: list of dates (YYYY-MM-DD)
        - prices: list of closing prices
        - volumes: list of volumes
        - scores: list of indicator scores
        - indicators: dict of indicator values (ma_5, ma_20, rsi)
    """
    # Get company object
    try:
        company = Company.objects.get(ticker=ticker)
    except Company.DoesNotExist:
        return JsonResponse({"error": "Company not found"}, status=404)
    
    # Get days parameter from request
    try:
        days = int(request.GET.get('days', 30))
    except ValueError:
        days = 30
    
    # Validate days range
    days = max(1, min(days, 700))  # Limit to 1-700 days
    
    # Calculate date range
    end_date = timezone.now()
    start_date = end_date - timedelta(days=days)
    
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
    
    # Create indicator dictionary for quick lookup
    indicator_dict = {}
    for indicator in indicators:
        date_str = indicator.date.strftime("%Y-%m-%d")
        indicator_dict[date_str] = {
            "score": float(indicator.score),
            "indicators": indicator.indicators
        }
    
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
    
    # Process daily quotes and match indicators
    for quote in daily_quotes:
        date_str = quote['date'].strftime("%Y-%m-%d")
        data["dates"].append(date_str)
        data["daily_data"].append({
            "open": float(quote['open']),
            "high": float(quote['high']),
            "low": float(quote['low']),
            "close": float(quote['close'])
        })
        data["volumes"].append(float(quote['volume']))
        
        # Get indicator for this date if available
        indicator = indicator_dict.get(date_str)
        if indicator:
            data["scores"].append(indicator["score"])
            ind_data = indicator["indicators"]
            data["indicators"]["ma_5"].append(ind_data.get("ma_5"))
            data["indicators"]["ma_20"].append(ind_data.get("ma_20"))
            data["indicators"]["rsi"].append(ind_data.get("rsi"))
        else:
            # Add placeholders for missing indicators
            data["scores"].append(None)
            data["indicators"]["ma_5"].append(None)
            data["indicators"]["ma_20"].append(None)
            data["indicators"]["rsi"].append(None)
    
    return JsonResponse(data)
