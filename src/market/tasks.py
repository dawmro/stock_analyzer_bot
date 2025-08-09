import helpers.clients as helper_clients
import math

from celery import shared_task
from django.apps import apps
from django.utils import timezone
from datetime import timedelta, timezone as datetime_timezone
from .services import get_stock_indicators
from .utils import batch_insert_stock_data, batch_insert_stock_indicators


@shared_task
def sync_company_stock_quotes(company_id, days_ago=32, batch_days_size=32, date_format="%Y-%m-%d", verbose=True):
    """
    Fetches and saves stock quote data for a specific company from Polygon API.
    
    This function is designed to fetch stock data in batches that stay within Polygon's API limits.
    The default 32-day batch size ensures we stay under the 50,000 data point limit per API call.
    
    Why 32 days?
    - 1 datapoint per minute = 1,440 per day
    - 32 days * 1,440 = 46,080 datapoints
    - Adding one extra day brings us to 33 days (47,520 datapoints)
    - This keeps us safely under Polygon's 50,000 datapoint limit per call
    
    Parameters:
        company_id (int): Primary key of the Company to sync
        days_ago (int): Number of days back to start fetching data (default=32)
        batch_days_size (int): Number of days to fetch per batch (default=32)
        date_format (str): Format for date strings in API requests (default='%Y-%m-%d')
        verbose (bool): If True, prints progress messages
        
    Raises:
        Exception: If company_id is invalid or company has no ticker
    """
 
    # Avoid circular imports by using apps.get_model
    Company = apps.get_model('market', 'Company')
    
    try:
        company_obj = Company.objects.get(id=company_id)
    except Company.DoesNotExist:
        # Invalid company ID provided
        raise Exception(f"Company Id: {company_id} invalid.")
    
    ticker = company_obj.ticker
    if not ticker:
        # Company record exists but has no ticker symbol
        raise Exception(f"Company ticker: {ticker} invalid.")

    # API configuration - using minute-level data
    multiplier = 1
    timespan = "minute"
    
    # Calculate date range for API request
    now = timezone.now()
    start_date = now - timedelta(days=days_ago)
    from_date = start_date.strftime(date_format)
    
    # Add extra day to ensure we cover full date range (Polygon's range is exclusive of end date)
    to_date = start_date + timedelta(days=batch_days_size + 1)
    to_date = to_date.strftime(date_format)

    # Initialize Polygon API client and fetch data
    client = helper_clients.PolygonAPIClient(
        ticker=ticker, 
        multiplier=multiplier,
        timespan=timespan,
        from_date=from_date, 
        to_date=to_date
    )
    dataset = client.get_stock_data()
    
    if verbose:
        print(f"Syncing {len(dataset)} stock quotes for {ticker} from {from_date} to {to_date}...")
    
    # Insert fetched data into database
    batch_insert_stock_data(dataset=dataset, company_obj=company_obj, verbose=verbose)
    
    if verbose:
        print(f"Done syncing {len(dataset)} stock quotes for {ticker} from {from_date} to {to_date}.")



@shared_task    
def sync_stock_data(days_ago=2):
    """
    Triggers stock data sync for all active companies.
    
    This task schedules sync jobs for each active company in the database,
    typically used for regular updates of recent stock data.
    
    Parameters:
        days_ago (int): Number of days back to fetch data (default=2 days)
    """
    # Get all active companies
    Company = apps.get_model('market', 'Company')
    companies = Company.objects.filter(active=True).values_list('id', flat=True)
    
    # Schedule sync task for each company
    for company_id in companies:
        sync_company_stock_quotes.delay(company_id, days_ago=days_ago)

        
@shared_task    
def sync_historical_stock_data(years_ago=2, company_ids=[], verbose=True):
    """
    Fetches historical stock data by breaking it into manageable chunks.
    
    Important: Polygon free tier only provides 2 years of historical data.
    Attempting to fetch beyond your plan limit will result in authorization errors.
    
    Approach:
    1. Breaks the historical period into 30-day batches
    2. Schedules sync tasks for each batch
    3. Uses approximate leap year calculation (365.25 days/year)
    
    Note: The loop has an off-by-one issue that misses the most recent period
    and may skip partial chunks at the end of the date range.
    
    Parameters:
        years_ago (int): Number of years of historical data to fetch (default=2)
        company_ids (list): Specific company IDs to sync (empty for all active)
        verbose (bool): If True, prints progress messages
    """
    # Polygon API plan limitations note:
    # Free users: 2 years historical data
    # Paid plans: 5, 10 or up to 20 years based on subscription
    # Exceeding plan limits returns:
    #   {
    #     "status": "NOT_AUTHORIZED",
    #     "request_id": "abc...123",
    #     "message": "Your plan doesn't include this data timeframe. Please upgrade at https://polygon.io/pricing"
    #   }

    # Get companies to process (all active or specific IDs)
    Company = apps.get_model('market', 'Company')
    qs = Company.objects.filter(active=True)
    
    if company_ids:  # Filter to specific companies if provided
        qs = qs.filter(id__in=company_ids)
        
    companies = qs.values_list('id', flat=True)

    for company_id in companies:
        # Calculate total days to fetch (accounting for leap years)
        starting_days_ago = years_ago * 365 + math.floor(years_ago/4)  # Approximate leap years
        
        batch_days_size = 30  # 30-day chunks to stay under API limits
        
        # Schedule sync tasks in reverse chronological order
        # Note: This loop has an off-by-one issue:
        #   - Misses the most recent period (0-30 days)
        #   - May skip a partial chunk at the end of the range
        for i in range(batch_days_size, starting_days_ago, batch_days_size):
            if verbose:
                print(f"Starting syncing {company_id} stock quotes from {i-batch_days_size+1} to {i} days ago...")
            
            # Schedule async task for this date range
            sync_company_stock_quotes.delay(
                company_id, 
                days_ago=i, 
                batch_days_size=batch_days_size
            )

@shared_task
def generate_historical_indicators(n_days=700, verbose=True):
    """
    Generate historical stock indicators for all active companies
    by running get_stock_indicators for each day going back n_days.
    Skips days that already have indicator data and uses batch insertion.
    """
    # Avoid circular imports by using apps.get_model
    Company = apps.get_model('market', 'Company')
    StockIndicator = apps.get_model('market', 'StockIndicator')
    
    if verbose:
        print("Starting historical indicators generation...")
    
    # Get all active companies
    companies = Company.objects.filter(active=True)
    if verbose:
        print(f"Processing {companies.count()} active companies")
    
    # Calculate date range (n_days back from now)
    end_date = timezone.now()
    start_date = end_date - timedelta(days=n_days)
    total_days = (end_date - start_date).days + 1
    
    if verbose:
        print(f"Processing {total_days} days from {start_date} to {end_date}")
    
        # Prefetch existing indicator dates as date objects (without time)
        existing_dates = {}
        for company in companies:
            dates = set(StockIndicator.objects.filter(
                company=company,
                time__date__range=(start_date.date(), end_date.date())
            ).dates('time', 'day'))
            existing_dates[company.id] = dates
        
        # Iterate through each company
        for company in companies:
            if verbose:
                print(f"Processing company: {company.ticker}")
            
            indicators_to_insert = []
            skipped_days = 0
            
            # Get existing dates for this company
            company_existing_dates = existing_dates.get(company.id, set())
            
            # Iterate through each day in range for this company
            current_date = start_date
            day_count = 0
            while current_date <= end_date:
                day_count += 1
                # Print progress every 100 day
                if verbose and day_count % 100 == 0:
                    print(f"Processed {day_count} days for {company.ticker} so far...")
                
                # Check if indicator already exists using pre-fetched data
                if current_date.date() in company_existing_dates:
                    skipped_days += 1
                    current_date += timedelta(days=1)
                    continue
                    
                try:
                    # Set evaluation time to end of day in UTC
                    eval_time = current_date.replace(
                        hour=23, minute=59, second=59, microsecond=0, tzinfo=datetime_timezone.utc
                    )
                    # Get indicators as of this historical date
                    result = get_stock_indicators(
                        ticker=company.ticker,
                        days=30,  # Use default lookback period
                        as_of_date=eval_time
                    )
                    
                    if result is None:
                        if verbose:
                            print(f"Skipping {company.ticker} on {current_date}: get_stock_indicators returned None")
                        continue
                    
                    # Prepare data for batch insertion
                    indicators_to_insert.append({
                        'time': eval_time,
                        'score': result['score'],
                        'indicators': result['indicators']
                    })
                    
                except Exception as e:
                    # Handle errors per company/date
                    print(f"Error for {company.ticker} on {current_date}: {str(e)}")
                
                current_date += timedelta(days=1)
            
            # Batch insert indicators for this company
            if indicators_to_insert:
                processed = batch_insert_stock_indicators(
                    indicators_to_insert, 
                    company_obj=company,
                    verbose=verbose
                )
                if verbose:
                    print(f"Inserted {processed} indicators for {company.ticker}")
            
            if verbose:
                print(f"Skipped {skipped_days} days for {company.ticker} (already exists)")
        
    if verbose:
        print("Finished generating historical indicators")
