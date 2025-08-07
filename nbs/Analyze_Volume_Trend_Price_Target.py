from setup import init_django
init_django()

from market.models import StockQuote
from django.db.models import Avg, F, RowRange, Window, Max, Min, Sum
from django.db.models.functions import TruncDate, FirstValue
from django.utils import timezone
from datetime import timedelta
from decimal import Decimal
from typing import List, Dict, Optional
from timescale.db.models.aggregates import Last  # Added for proper last value aggregation

# Constants for analysis configuration
DAYS_AGO = 700  # Analysis period in days
TICKER = "X:BTCUSD"  # Default ticker symbol to analyze

# Calculate time range for analysis
now = timezone.now()
start_date = now - timedelta(days=DAYS_AGO)
end_date = now

def get_volume_trend_datapoint(queryset, days: int = 5) -> Optional[Dict[str, float]]:
    """
    Calculate volume trend metrics for the latest datapoint in the queryset.
    
    Uses a window function to compute:
    - Average volume over the specified number of days
    - Latest volume
    - Percentage change between latest and average volume
    
    Args:
        queryset: QuerySet of StockQuote objects
        days: Number of days to include in moving average (default: 5)
    
    Returns:
        Dictionary with metrics or None if insufficient data
    """
    # Validate we have enough data points
    if queryset.count() < days:
        return None
    
    # Window function to calculate moving average of volume
    # Frame spans from (current row - days + 1) to current row
    # Ordered by time ascending to ensure chronological order
    annotated_qs = queryset.annotate(
        avg_volume=Window(
            expression=Avg('volume'),
            order_by=F('time').asc(),
            partition_by=[],  # No partitioning - treat as single time series
            frame=RowRange(start=-(days - 1), end=0),
        )
    ).order_by('-time')  # Order descending to get latest first
    
    # Get the latest data point
    data_point = annotated_qs.first()
    
    if not data_point or data_point.avg_volume is None:
        return None
    
    # Calculate percentage change from average
    volume_change = ((data_point.volume - data_point.avg_volume) / data_point.avg_volume) * 100

    return {
        'avg_volume_datapoint': float(data_point.avg_volume),
        'latest_volume_datapoint': float(data_point.volume),
        'volume_change_percent_datapoint': float(volume_change),
    }

def get_volume_trend_daily(queryset, days: int = 5) -> Optional[Dict[str, float]]:
    """
    Calculate daily volume trend metrics using aggregated daily volumes.
    
    Compares the latest day's volume against the average of previous N days.
    More efficient than per-datapoint as it works with daily aggregates.
    
    Args:
        queryset: QuerySet of StockQuote objects
        days: Number of previous days for comparison (default: 5)
    
    Returns:
        Dictionary with metrics or None if insufficient data
    """
    # Aggregate volumes by day
    daily_volumes = queryset.annotate(date=TruncDate('time')) \
        .values('date') \
        .annotate(daily_volume=Sum('volume')) \
        .order_by('-date')
    
    # Convert to list for slicing
    daily_list = list(daily_volumes)
    
    # Ensure we have enough data (current day + previous days)
    if len(daily_list) < days + 1:
        return None
    
    # Latest day's volume
    latest = daily_list[0]
    latest_volume = latest['daily_volume']
    
    # Calculate average of previous N days (excluding current day)
    prev_days = daily_list[1:days+1]
    avg_volume = sum(day['daily_volume'] for day in prev_days) / days
    
    # Calculate percentage change
    volume_change = ((latest_volume - avg_volume) / avg_volume) * 100

    return {
        'avg_volume_daily': float(avg_volume),
        'latest_volume_daily': float(latest_volume),
        'volume_change_percent_daily': float(volume_change),
    }

def get_simple_target(ticker: str, timestamps: List = [], days: int = 180) -> Optional[Dict[str, float]]:
    """
    Calculate simplified price targets using historical price data.
    
    Methodology:
    1. Gets daily aggregated data (high, low, close)
    2. Uses Fibonacci extensions (38.2% and 61.8%) of recent price range
       added to current price to establish targets
    
    Args:
        ticker: Stock ticker symbol
        timestamps: Optional specific timestamps to include
        days: Lookback period in days (default: 180)
    
    Returns:
        Dictionary with price targets and metrics or None if no data
    """
    end_date = timezone.now()
    start_date = end_date - timedelta(days=days)
    
    # Build filter conditions
    lookups = {
        "company__ticker": ticker,
        "time__range": (start_date, end_date)
    }
    if timestamps:
        lookups['time__in'] = timestamps
    
    # Get daily aggregated data using TimescaleDB hyperfunctions
    daily_data = (
        StockQuote.timescale
        .filter(**lookups)
        .time_bucket('time', '1 day')
        # Properly aggregate daily metrics
        .annotate(
            daily_high=Max('high_price'),
            daily_low=Min('low_price'),
            daily_close=Last('close_price', 'time'),  # Last price of the day
        )
    )
    
    # Get all daily aggregates as list
    daily_list = list(daily_data)
    if not daily_list:
        return None
    
    # Get current price (first available close price)
    current_price = daily_list[0]['daily_close']
    print("Current Price:", current_price)
    
    # Calculate overall price range in the period
    highest = max(day['daily_high'] for day in daily_list)
    lowest = min(day['daily_low'] for day in daily_list)
    price_range = float(highest - lowest)
    
    # Calculate average price
    avg_price = sum(day['daily_close'] for day in daily_list) / len(daily_list)
    
    # Calculate targets using Fibonacci extensions
    conservative_target = current_price + (price_range * 0.382)
    aggressive_target = current_price + (price_range * 0.618)

    return {
        'current_price': float(current_price),
        'conservative_target': float(conservative_target),
        'aggressive_target': float(aggressive_target),
        'average_price': float(avg_price),
        'period_high': float(highest),
        'period_low': float(lowest)
    }

# Example usage with debugging
if __name__ == "__main__":
    # Get actual timestamps for daily buckets
    latest_daily = (
        StockQuote.timescale.filter(company__ticker=TICKER, time__range=(start_date, end_date))
        .time_bucket('time', '1 day')
        .annotate(latest_time=Max('time'))
        .values_list('latest_time', flat=True)
    )
    actual_timestamps = sorted(set(latest_daily))
    print(f"First 3 timestamps: {actual_timestamps[:3]}")
    
    # Get full queryset
    qs = StockQuote.objects.filter(
        company__ticker=TICKER, 
        time__range=(start_date, end_date)
    )
    
    # Print sample data points
    print("\nFirst 3 quotes:")
    for quote in qs[:3]:
        print(quote.time, quote.close_price)
        
    print("\nLast 3 quotes:")
    # Get last 3 using reverse ordering and slicing
    last_three = qs.order_by('-time')[:3]
    for quote in reversed(last_three):
        print(quote.time, quote.close_price)
    
    # Calculate and print volume trends
    print("\nVolume Trend (Datapoint):")
    print(get_volume_trend_datapoint(queryset=qs, days=5))
    
    print("\nVolume Trend (Daily):")
    print(get_volume_trend_daily(queryset=qs, days=5))
    
    # Calculate and print price target
    print("\nPrice Target:")
    print(get_simple_target(ticker=TICKER, days=365))
