import helpers.clients as helper_clients
import math

from celery import shared_task
from django.apps import apps
from django.utils import timezone

from .utils import batch_insert_stock_data
from datetime import timedelta

    

@shared_task
def sync_company_stock_quotes(company_id, days_ago=32, batch_days_size=32, date_format="%Y-%m-%d", verbose=True):
    # 32 days is max safe value because 1 datapoint per minute equals 1440 per day, is 46080 per 32 days,
    # in function logic we add one more day to the end of range, timedelta can add another, so 48960 days,
    # which is less than the limit of 50000 datapoint per call from Polygon API, 
    # yet it always covers at least entire month of data.
 
    Company = apps.get_model('market', 'Company')
    try:
        company_obj = Company.objects.get(id=company_id)
    except Company.DoesNotExist:
        company_obj = None
    if not company_obj:
        raise Exception(f"Company Id: {company_id} invalid.")
    ticker = company_obj.ticker
    if not ticker:
        raise Exception(f"Company ticker: {ticker} invalid.")

    multiplier = 1
    timespan = "minute"
    now = timezone.now()
    start_date = now - timedelta(days=days_ago)
    from_date = start_date.strftime(date_format)
    to_date = start_date + timedelta(days=batch_days_size + 1)
    to_date = to_date.strftime(date_format)

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
    batch_insert_stock_data(dataset=dataset, company_obj=company_obj, verbose=verbose)
    if verbose:
        print(f"Done syncing {len(dataset)} stock quotes for {ticker} from {from_date} to {to_date}.")


@shared_task    
def sync_stock_data(days_ago=2):
    Company = apps.get_model('market', 'Company')
    companies = Company.objects.filter(active=True).values_list('id', flat=True)
    for company_id in companies:
        sync_company_stock_quotes.delay(company_id, days_ago=days_ago)

        
@shared_task    
def sync_historical_stock_data(years_ago=2, company_ids=[],verbose=True):
    # Polygon API plan is limited to 2 year of data for free users. 
    # Paid users can get 5, 10 or up to 20 years of data based on their plan.
    # Exceeding the limit will result in a following response instead of data:
    # {
    #     "status": "NOT_AUTHORIZED",
    #     "request_id": "abc...123",
    #     "message": "Your plan doesn't include this data timeframe. Please upgrade your plan at https://polygon.io/pricing"
    # }

    Company = apps.get_model('market', 'Company')
    qs = Company.objects.filter(active=True)
    if len(company_ids) > 0:
        qs = qs.filter(id__in=company_ids)
    companies = qs.values_list('id', flat=True)

    for company_id in companies:
        starting_days_ago = years_ago * 365 + math.floor(years_ago/4) # leap years
        batch_days_size = 30
        for i in range(batch_days_size, starting_days_ago, batch_days_size):
            if verbose:
                print(f"Starting syncing {company_id} stock quotes from {i-batch_days_size+1} to {i} days ago...")
            sync_company_stock_quotes.delay(company_id, days_ago=i, batch_days_size=batch_days_size)





