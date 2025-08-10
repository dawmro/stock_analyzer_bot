import json
from datetime import timedelta
from django.core.cache import cache
from django.conf import settings
from openai import OpenAI
from market import services as market_services

def get_llm_recommendation(ticker):
    # Check cache first
    cache_key = f"llm_rec_{ticker}"
    if cached := cache.get(cache_key):
        return cached
    
    # Get latest indicators (last 30 days)
    queryset = market_services.get_stock_quotes_queryset(ticker, days=30)
    
    if queryset.count() == 0:
        return None
    
    # Calculate indicators
    averages = market_services.get_daily_moving_averages(ticker)
    price_target = market_services.get_price_target(ticker, days=30, queryset=queryset)
    volume_trend_daily = market_services.get_volume_trend_daily(ticker, days=30, queryset=queryset)
    rsi_data = market_services.calculate_rsi(ticker)
    
    # Prepare data for OpenAI
    indicators = {
        "ticker": ticker,
        "days": 30,
        "indicators": {
            **averages,
            **price_target,
            **volume_trend_daily,
            **rsi_data,
        }
    }
    
    try:
        # Call OpenAI API
        client = OpenAI(api_key=settings.OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": "You are an expert at analyzing stocks. Respond in JSON format with properties: buy (boolean), sell (boolean), hold (boolean), explanation (string)"
                },
                {
                    "role": "user", 
                    "content": f"Considering these technical indicators: {json.dumps(indicators)}, provide a recommendation"
                }
            ],
            response_format={"type": "json_object"}
        )
        
        # Parse and cache result
        result = json.loads(response.choices[0].message.content)
        cache.set(cache_key, result, 60 * 60 * 12)  # Cache for 12 hours
        return result
    except Exception as e:
        print(f"Error generating recommendation: {str(e)}")
        return None
