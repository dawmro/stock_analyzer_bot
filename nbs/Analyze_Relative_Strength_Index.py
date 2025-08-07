#!/usr/bin/env python
# coding: utf-8

import setup
setup.init_django()

from market.models import StockQuote

from django.db.models import (
    Avg, 
    F,
    Window,
    ExpressionWrapper,
    DecimalField,
    Case,
    When,
    Value,
    Count
)
from django.db.models.functions import Lag, Coalesce
from django.utils import timezone
from datetime import timedelta
from decimal import Decimal


def calculate_rsi(ticker, period=14):
    """
    Calculate Relative Strength Index (RSI) using Django ORM with TimescaleDB.
    
    RSI is calculated using the following steps:
    1. Retrieve daily closing prices for the given ticker
    2. Calculate daily price changes
    3. Separate gains (positive changes) and losses (negative changes, stored as positives)
    4. Compute initial averages:
        - First average gain = average of gains in first period
        - First average loss = average of losses in first period
    5. Calculate exponential moving averages (EMA) for subsequent periods:
        EMA_gain = (prev_EMA_gain * (period-1) + current_gain) / period
        EMA_loss = (prev_EMA_loss * (period-1) + current_loss) / period
    6. Compute Relative Strength (RS) = EMA_gain / EMA_loss
    7. RSI = 100 - (100 / (1 + RS))
    
    Uses Wilder's smoothing method (EMA variant) which is standard for RSI calculation.
    
    Args:
        ticker (str): Stock ticker symbol
        period (int): Number of periods for RSI calculation (default: 14)
        
    Returns:
        dict: Contains RSI value, average gain, average loss, and period
               Returns error message if insufficient data
    """
    
    # Validate period input
    if period < 1:
        return {
            'error': 'Invalid period: must be at least 1',
            'period': period
        }
    
    end_date = timezone.now()
    start_date = end_date - timedelta(days=period * 4)  # Fetch extra data for initial calculations
    
    # Get daily price data using TimescaleDB time_bucket
    daily_data = (
        StockQuote.timescale
        .filter(company__ticker=ticker, time__range=(start_date, end_date))
        .time_bucket('time', '1 day')
        .order_by('bucket')
    )

    # Calculate price changes and gains/losses
    # Using ExpressionWrapper to ensure proper decimal precision
    movement = daily_data.annotate(
        closing_price=ExpressionWrapper(
            F('close_price'),
            output_field=DecimalField(max_digits=10, decimal_places=4)
        ),
        # Get previous close using Lag window function
        prev_close=Window(
            expression=Lag('close_price', default=None),
            order_by=F('bucket').asc(),
            partition_by=[],
            output_field=DecimalField(max_digits=10, decimal_places=4)
        )
    ).annotate(
        # Calculate price change from previous day
        price_change=ExpressionWrapper(
            F('close_price') - Coalesce(F('prev_close'), F('close_price')),  # Handle first day
            output_field=DecimalField(max_digits=10, decimal_places=4)
        ),
        # Calculate gain (positive changes)
        gain=Case(
            When(price_change__gt=0, 
                 then=ExpressionWrapper(
                     F('price_change'),
                     output_field=DecimalField(max_digits=10, decimal_places=4)
                 )),
            default=Value(0, output_field=DecimalField(max_digits=10, decimal_places=4)),
            output_field=DecimalField(max_digits=10, decimal_places=4)
        ),
        # Calculate loss (negative changes converted to positive)
        loss=Case(
            When(price_change__lt=0,
                 then=ExpressionWrapper(
                     -F('price_change'),  # Convert loss to positive value
                     output_field=DecimalField(max_digits=10, decimal_places=4)
                 )),
            default=Value(0, output_field=DecimalField(max_digits=10, decimal_places=4)),
            output_field=DecimalField(max_digits=10, decimal_places=4)
        )
    )

    # Check if we have sufficient data (need at least period+1 days)
    valid_movement = movement.exclude(prev_close__isnull=True)
    if valid_movement.count() < period:
        return {
            'error': f'Insufficient data: need at least {period} days of valid data',
            'days_available': valid_movement.count(),
            'period': period
        }

    # Calculate initial averages using database aggregation
    initial_avg = valid_movement[:period].aggregate(
        avg_gain=Coalesce(
            ExpressionWrapper(
                Avg('gain'),
                output_field=DecimalField(max_digits=10, decimal_places=4)
            ),
            Value(0, output_field=DecimalField(max_digits=10, decimal_places=4))
        ),
        avg_loss=Coalesce(
            ExpressionWrapper(
                Avg('loss'),
                output_field=DecimalField(max_digits=10, decimal_places=4)
            ),
            Value(0, output_field=DecimalField(max_digits=10, decimal_places=4))
        )
    )

    avg_gain = initial_avg['avg_gain']
    avg_loss = initial_avg['avg_loss']
    
    # Calculate EMA for subsequent periods
    # Skip initial period used for SMA and process remaining data
    for i, data in enumerate(valid_movement):
        if i < period:
            continue  # Skip initial period used for SMA
        
        # EMA formula: current EMA = (prev_EMA * (period-1) + current_value) / period
        # Access data as dictionary since queryset iteration returns dicts
        avg_gain = (avg_gain * (period - 1) + data['gain']) / period
        avg_loss = (avg_loss * (period - 1) + data['loss']) / period

    # Handle case where average loss is zero to avoid division by zero
    if avg_loss == 0:
        rsi = 100.0  # All gains, no losses
    else:
        rs = avg_gain / avg_loss  # Relative Strength
        rsi = 100 - (100 / (1 + rs))

    return {
        'rsi': round(float(rsi), 4),
        'avg_gain': round(float(avg_gain), 4),
        'avg_loss': round(float(avg_loss), 4),
        'period': period
    }


# Test code only runs when executed directly, not when imported
if __name__ == '__main__':
    # Example usage
    rsi_data = calculate_rsi('X:BTCUSD')
    print("RSI Calculation Results:")
    print(rsi_data)
