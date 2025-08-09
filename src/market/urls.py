from django.urls import path
from . import views

urlpatterns = [
    path('api/stock-data/<str:ticker>/', views.stock_data_api, name='stock_data_api'),
    path('chart/', views.stock_chart_view, name='stock_chart'),
]
