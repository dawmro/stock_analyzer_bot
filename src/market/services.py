from django.db.models import (
    Avg, 
    F,
    RowRange,
    Window,
    Max, 
    Min, 
    Sum,
    ExpressionWrapper,
    DecimalField,
    Case,
    When,
    Value,
)
from django.db.models.functions import TruncDate, Lag, Coalesce
from django.utils import timezone
from datetime import timedelta
from typing import Dict, Optional
from timescale.db.models.aggregates import Last  # For last value aggregation
from decimal import Decimal

from market.models import StockQuote


# Constants for signal thresholds
VOLUME_CHANGE_THRESHOLD = 20
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

def get_stock_quotes_queryset(ticker: str, days: int = 28):
    """
    Retrieve stock quotes for a given ticker within a date range.
    
    Args:
        ticker: Stock ticker symbol
        days: Number of days to retrieve (default: 28)
    
    Returns:
        QuerySet of StockQuote objects
    """
    now = timezone.now()
    start_date = now - timedelta(days=days)
    end_date = now

    # Get full queryset (Ensure we have enough data +1 day)
    qs = StockQuote.objects.filter(
        company__ticker=ticker, 
        time__range=(start_date - timedelta(days=1), end_date)
    )
    return qs

def get_volume_trend_datapoint(ticker: str, days: int = 5, queryset=None) -> Optional[Dict[str, float]]:
    """
    Calculate volume trend metrics for the latest datapoint in the queryset.
    
    Uses a window function to compute:
    - Average volume over the specified number of days
    - Latest volume
    - Percentage change between latest and average volume
    
    Args:
        ticker: Ticker symbol for the stock
        days: Number of days to include in moving average (default: 5)
        queryset: QuerySet of StockQuote objects
    
    Returns:
        Dictionary with metrics or None if insufficient data
    """
    if queryset is None:
        queryset = get_stock_quotes_queryset(ticker=ticker, days=days)

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


def get_volume_trend_daily(ticker: str, days: int = 5, queryset=None) -> Optional[Dict[str, float]]:
    """
    Calculate daily volume trend metrics using aggregated daily volumes.
    
    Compares the latest day's volume against the average of previous N days.
    More efficient than per-datapoint as it works with daily aggregates.
    
    Args:
        ticker: Ticker symbol of the stock
        days: Number of previous days for comparison (default: 5)
        queryset: QuerySet of StockQuote objects
    
    Returns:
        Dictionary with metrics or None if insufficient data
    """
    if queryset is None:
        queryset = get_stock_quotes_queryset(ticker=ticker, days=days)

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


def get_price_target(ticker: str, days: int = 180, queryset=None) -> Optional[Dict[str, float]]:
    """
    Calculate price targets using Fibonacci extensions.
    
    Methodology:
    1. Get daily aggregated data (high, low, close) using the helper function
    2. Calculate price range over period
    3. Apply Fibonacci ratios (38.2%, 61.8%) to current price
    
    Args:
        ticker: Stock ticker
        days: Lookback period in days (default: 180)
        queryset: Optional pre-filtered queryset
    
    Returns:
        Price targets dict or None if no data
    """
    if queryset is None:
        queryset = get_stock_quotes_queryset(ticker=ticker, days=days)
    daily_data = (
        queryset
        .annotate(date=TruncDate('time'))  # Create date field for grouping
        .values('date')  # Group by date
        .annotate(
            daily_high=Max('high_price'),
            daily_low=Min('low_price'),
            daily_close=Last('close_price', 'time'),  # Last price of day
        )
        .order_by('-date')  # Descending for latest date
    )
    
    # Convert to list
    daily_list = list(daily_data)
    if not daily_list:
        return None
    
    # Get current price (latest close)
    current_price = daily_list[0]['daily_close']
    
    # Calculate price range
    highest = max(day['daily_high'] for day in daily_list)
    lowest = min(day['daily_low'] for day in daily_list)
    
    # Handle zero range case
    if highest == lowest:
        return {
            'current_price': float(current_price),
            'conservative_target': float(current_price),
            'aggressive_target': float(current_price),
            'average_price': float(current_price),
            'period_high': float(highest),
            'period_low': float(lowest)
        }
    
    price_range = float(highest - lowest)
    
    # Calculate average price
    avg_price = sum(day['daily_close'] for day in daily_list) / len(daily_list)
    
    # Calculate Fibonacci targets
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

def get_daily_moving_averages(ticker: str, days: int = 200, queryset=None) -> Dict[str, Optional[float]]:
    """
    Calculate moving averages (5-day, 20-day, 50-day, 100-day, 200-day) using the last closing price of each day.

    Args:
        ticker: Stock ticker
        days: Days to retrieve (default: 200)
        queryset: Optional pre-filtered queryset

    Returns:
        Moving averages dict with values (or None if insufficient data) for each window.
    """
    if queryset is None:
        queryset = get_stock_quotes_queryset(ticker=ticker, days=days)
    
    # Define window sizes and corresponding output keys
    window_specs = [
        (5, 'ma_5'),
        (20, 'ma_20'),
        (50, 'ma_50'),
        (100, 'ma_100'),
        (200, 'ma_200')
    ]
    
    # Aggregate to get the last closing price for each day
    daily_data = (
        queryset
        .annotate(date=TruncDate('time'))
        .values('date')
        .annotate(daily_close=Last('close_price', 'time'))
        .order_by('-date')  # Latest dates first
    )
    
    # Get list of daily closes (most recent first)
    daily_closes = [entry['daily_close'] for entry in daily_data]
    
    # Prepare results dictionary
    results = {}
    
    # Calculate each moving average
    for window_size, key in window_specs:
        if len(daily_closes) >= window_size:
            # Take the most recent 'window_size' days
            window_values = daily_closes[:window_size]
            avg = sum(window_values) / window_size
            results[key] = float(round(avg, 4))
        else:
            results[key] = None
    
    return results


def calculate_rsi(ticker: str, period: int = 14) -> Dict:
    """
    Calculate Relative Strength Index (RSI) using Wilder's method.
    
    Steps:
    1. Get daily closing prices
    2. Calculate daily price changes
    3. Separate gains and losses
    4. Compute initial SMA for gains/losses
    5. Calculate EMA for subsequent periods
    6. Compute RSI = 100 - (100 / (1 + RS))
    
    Args:
        ticker: Stock ticker
        period: RSI period (default: 14)
    
    Returns:
        RSI data dict with possible error message
    """
    # Validate period
    if period < 1:
        return {
            'error': 'Invalid period: must be at least 1',
            'period': period
        }
    
    end_date = timezone.now()
    start_date = end_date - timedelta(days=period * 4)  # Extra data buffer
    
    # Get daily data with time_bucket
    daily_data = (
        StockQuote.timescale
        .filter(company__ticker=ticker, time__range=(start_date, end_date))
        .time_bucket('time', '1 day')
        .order_by('bucket')
    )

    # Calculate price changes
    movement = daily_data.annotate(
        # Get previous close
        prev_close=Window(
            expression=Lag('close_price', default=None),
            order_by=F('bucket').asc(),
            partition_by=[],
            output_field=DecimalField(max_digits=10, decimal_places=4)
        )
    ).annotate(
        # Calculate price change
        price_change=ExpressionWrapper(
            F('close_price') - Coalesce(F('prev_close'), F('close_price')),
            output_field=DecimalField(max_digits=10, decimal_places=4)
        ),
        # Calculate gain (positive changes)
        gain=Case(
            When(price_change__gt=0, then=F('price_change')),
            default=Value(0),
            output_field=DecimalField(max_digits=10, decimal_places=4)
        ),
        # Calculate loss (negative changes as positive)
        loss=Case(
            When(price_change__lt=0, then=-F('price_change')),
            default=Value(0),
            output_field=DecimalField(max_digits=10, decimal_places=4)
        )
    )

    # Check data sufficiency
    valid_movement = movement.exclude(prev_close__isnull=True)
    if valid_movement.count() < period:
        return {
            'error': f'Insufficient data: need at least {period} days of valid data',
            'days_available': valid_movement.count(),
            'period': period
        }

    # Calculate initial SMA with proper output field types and convert to Decimal
    initial_avg = valid_movement[:period].aggregate(
        avg_gain=Coalesce(Avg('gain', output_field=DecimalField(max_digits=10, decimal_places=4)), 
                          Value(0, output_field=DecimalField(max_digits=10, decimal_places=4))),
        avg_loss=Coalesce(Avg('loss', output_field=DecimalField(max_digits=10, decimal_places=4)), 
                          Value(0, output_field=DecimalField(max_digits=10, decimal_places=4)))
    )
    
    # Ensure we use Decimal consistently
    avg_gain = initial_avg['avg_gain'] or Decimal('0.0')
    avg_loss = initial_avg['avg_loss'] or Decimal('0.0')
    
    # Calculate EMA for remaining periods
    period_decimal = Decimal(str(period))
    for i, data in enumerate(valid_movement):
        if i < period:  # Skip initial SMA period
            continue
        
        # Convert gain/loss to Decimal if needed
        current_gain = data['gain'] if isinstance(data['gain'], Decimal) else Decimal(str(data['gain']))
        current_loss = data['loss'] if isinstance(data['loss'], Decimal) else Decimal(str(data['loss']))
        
        # Wilder's EMA: (prev * (period-1) + current) / period
        # Use Decimal arithmetic throughout
        avg_gain = (avg_gain * (period_decimal - 1) + current_gain) / period_decimal
        avg_loss = (avg_loss * (period_decimal - 1) + current_loss) / period_decimal

    # Calculate RSI
    if avg_loss == 0:
        rsi = 100.0  # Avoid division by zero
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    return {
        'rsi': round(float(rsi), 4),
        'avg_gain': round(float(avg_gain), 4),
        'avg_loss': round(float(avg_loss), 4),
        'period': period
    }

def get_stock_indicators(ticker: str = "X:BTCUSD", days: int = 30) -> Dict:
    """
    Aggregate stock indicators into a single score.
    
    Combines:
    - Moving averages 
    - Price targets
    - Volume trends
    - RSI
    
    Args:
        ticker: Stock ticker (default: X:BTCUSD)
        days: Analysis period (default: 30)
    
    Returns:
        Aggregated indicator results with composite score
    """
    queryset = get_stock_quotes_queryset(ticker, days=days)
    if queryset.count() == 0:
        raise Exception(f"Data for {ticker} not found")
    
    # Get indicators with error handling
    averages = get_daily_moving_averages(ticker)
    price_target = get_price_target(ticker, days=days, queryset=queryset)
    volume_trend_daily = get_volume_trend_daily(ticker, days=days, queryset=queryset)
    rsi_data = calculate_rsi(ticker)
    signals = []
    
    # Moving average crossover signal
    if averages.get('ma_5') > averages.get('ma_20'):
        signals.append(1) # Bullish
    else:
        signals.append(-1) # Bearish

    if averages.get('ma_20') > averages.get('ma_50'):
        signals.append(1) # Bullish
    else:
        signals.append(-1) # Bearish

    if averages.get('ma_50') > averages.get('ma_100'):
        signals.append(1) # Bullish
    else:
        signals.append(-1) # Bearish

    if averages.get('ma_100') > averages.get('ma_200'):
        signals.append(1) # Bullish
    else:
        signals.append(-1) # Bearish
    
    # Price target signal
    current_price = price_target.get('current_price', 0)
    conservative_target = price_target.get('conservative_target', 0)
    if current_price < conservative_target:
        signals.append(1)  # Undervalued
    else:
        signals.append(-1)  # Overvalued
    
    # Volume trend signal
    volume_change = volume_trend_daily.get("volume_change_percent_daily", 0)
    if volume_change > VOLUME_CHANGE_THRESHOLD:
        signals.append(1)  # High volume surge
    elif volume_change < -VOLUME_CHANGE_THRESHOLD:
        signals.append(-1)  # High volume drop
    else:
        signals.append(0)  # Neutral
    
    # RSI signal
    rsi = rsi_data.get('rsi', 50)
    if rsi > RSI_OVERBOUGHT:
        signals.append(-1)  # Overbought
    elif rsi < RSI_OVERSOLD:
        signals.append(1)  # Oversold
    else:
        signals.append(0)  # Neutral
    
    return {
        "days": days,
        "score": sum(signals),
        "ticker": ticker,
        "indicators": {
            **averages,
            **price_target,
            **volume_trend_daily,
            **rsi_data,
        }
    }
