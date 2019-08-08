from django.urls import path
from . import views

app_name = 'report'

urlpatterns = [
    path('', views.report, name='index'),
    path('dbinfo/<int:history_id>/', views.get_dbinfo_report, name='report'),
    path('major/<int:history_id>/', views.get_major_top_10_report, name='major_top'),
    path('sqlstat-only/<int:history_id>/', views.get_sql_stat_sql_only_report, name='sql_stat_only'),

]
