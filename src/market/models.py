from django.db import models
from timescale.db.models.fields import TimescaleDateTimeField
from timescale.db.models.managers import TimescaleManager
from . import tasks


# Create your models here.
class Company(models.Model):
    name = models.CharField(max_length=255)
    ticker = models.CharField(max_length=20, unique=True, db_index=True)
    description = models.TextField(blank=True, null=True)
    active = models.BooleanField(default=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    updated = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.ticker = f"{self.ticker}".upper()
        super().save(*args, **kwargs)
        tasks.sync_company_stock_quotes.delay(self.pk)


class StockQuote(models.Model):
    """
    'open_price': 93758,
    'close_price': 94757,
    'high_price': 95287,
    'low_price': 93072,
    'number_of_trades': 18517,
    'volume': 546.4188032799949,
    'volume_weighted_average': 93954.3074,
    'raw_timestamp': 987654323,
    'time': datetime.datetime(2025, 1, 1, 0, 0, tzinfo=<UTC>)
    """
    company = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        related_name="stock_quotes"
    )
    open_price = models.DecimalField(max_digits=10, decimal_places=4)
    close_price = models.DecimalField(max_digits=10, decimal_places=4)
    high_price = models.DecimalField(max_digits=10, decimal_places=4)
    low_price = models.DecimalField(max_digits=10, decimal_places=4)
    number_of_trades = models.BigIntegerField(blank=True, null=True)
    volume = models.DecimalField(max_digits=18, decimal_places=4)
    volume_weighted_average = models.DecimalField(max_digits=10, decimal_places=4)
    raw_timestamp = models.CharField(max_length=100, blank=True, null=True)
    time = TimescaleDateTimeField(interval="1 day")

    objects = models.Manager()
    timescale = TimescaleManager()

    class Meta:
        unique_together = [('company', 'time')]
    
 