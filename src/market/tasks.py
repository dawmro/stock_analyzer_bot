import helpers.clients as helper_clients

from celery import shared_task
from django.apps import apps
from django.utils import timezone

from .utils import batch_insert_stock_data
from datetime import timedelta

    

@shared_task
def sync_company_stock_quotes(company_id, days_ago=3, date_format="%Y-%m-%d", verbose=True):
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
    days_ago = 3
    end_date = timezone.now()
    start_date = end_date - timedelta(days=days_ago)
    from_date = start_date.strftime(date_format)
    to_date = end_date.strftime(date_format)

    client = helper_clients.PolygonAPIClient(
        ticker=ticker, 
        multiplier=multiplier,
        timespan=timespan,
        from_date=from_date, 
        to_date=to_date
    )
    dataset = client.get_stock_data()
    if verbose:
        print(f"Syncing {len(dataset)} stock quotes for {ticker}...")
    batch_insert_stock_data(dataset=dataset, company_obj=company_obj, verbose=verbose)


@shared_task    
def sync_stock_data():
    Company = apps.get_model('market', 'Company')
    companies = Company.objects.filter(active=True).values_list('id', flat=True)
    for company_id in companies:
        sync_company_stock_quotes.delay(company_id)


