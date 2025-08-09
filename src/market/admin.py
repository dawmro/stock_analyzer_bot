import zoneinfo
from django.contrib import admin
from django.utils import timezone
from rangefilter.filters import (
    DateTimeRangeFilterBuilder,
)

# Register your models here.
from .models import Company, StockQuote


class CompanyAdmin(admin.ModelAdmin):
    list_display = ['ticker', 'name', 'timestamp', 'updated', 'active']
    search_fields = ['ticker', 'name']

admin.site.register(Company, CompanyAdmin)

class StockQuoteAdmin(admin.ModelAdmin):
    list_display = ['company_ticker', 'close_price', 'volume', 'number_of_trades', 'time', 'localized_time']
    list_filter = [
        'company__ticker', 
        ('time', DateTimeRangeFilterBuilder()),
        'time'
        ]
    readonly_fields = ['localized_time', 'raw_timestamp', 'time']
    
    def company_ticker(self, obj):
        return obj.company.ticker
    company_ticker.short_description = 'Ticker'
    company_ticker.admin_order_field = 'company__ticker'

    def localized_time(self, obj): 
        tz_name = "Poland"
        user_tz = zoneinfo.ZoneInfo(tz_name)
        local_time = obj.time.astimezone(user_tz)
        return local_time.strftime("%b %d, %Y, %I:%M %p (%Z)")
    
    def get_queryset(self, request):
        tz_name = "UTC"
        user_tz = zoneinfo.ZoneInfo(tz_name)
        timezone.activate(user_tz)
        return super().get_queryset(request)



from .models import StockIndicator

class StockIndicatorAdmin(admin.ModelAdmin):
    list_display = ['company_ticker', 'time', 'score']
    list_filter = [
        'company__ticker', 
        ('time', DateTimeRangeFilterBuilder()),
    ]
    readonly_fields = ['indicators']
    
    def company_ticker(self, obj):
        return obj.company.ticker
    company_ticker.short_description = 'Ticker'
    company_ticker.admin_order_field = 'company__ticker'

admin.site.register(StockQuote, StockQuoteAdmin)
admin.site.register(StockIndicator, StockIndicatorAdmin)
