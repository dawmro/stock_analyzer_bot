from django.contrib import admin

# Register your models here.
from .models import Company, StockQuote


class CompanyAdmin(admin.ModelAdmin):
    list_display = ['ticker', 'name', 'timestamp', 'updated', 'active']
    search_fields = ['ticker', 'name']

admin.site.register(Company, CompanyAdmin)

class StockQuoteAdmin(admin.ModelAdmin):
    list_display = ['company_ticker', 'close_price', 'volume', 'number_of_trades', 'time']
    list_filter = ['company__ticker', 'time']
    
    def company_ticker(self, obj):
        return obj.company.ticker
    company_ticker.short_description = 'Ticker'
    company_ticker.admin_order_field = 'company__ticker'


admin.site.register(StockQuote, StockQuoteAdmin)
