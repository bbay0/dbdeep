from django.urls import path
from . import views

app_name = 'dbconn'

urlpatterns = [
    path('create/', views.create_chart, name='create_chart'),
    path('history/', views.HistoryAPIView.as_view(), name='history_list')
]
