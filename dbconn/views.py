from django.core import serializers
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_POST, require_GET

from rest_framework.decorators import api_view
from rest_framework.views import APIView
from rest_framework.response import Response

from util.database import connection
from util.decorators import ajax_required
from .forms import DBconnForm
from .models import DBconn, History
from .serializers import HistorySerializer 

def index(request):
    taplate_name = 'index.html'
    return render(request, taplate_name)


class HistoryAPIView(APIView):

    def get(self, request, format=None):
        history_list = History.objects.all()
        serializer = HistorySerializer(history_list, many=True)
        return Response(serializer.data)

@ajax_required
@api_view(['POST'])
def create_chart(request):
    username = request.POST['username']
    password = request.POST['password']
    server_ip = request.POST['server_ip']
    port = request.POST['port']
    print(username, password, server_ip, port)
    # start_date = request.POST['start_date']
    start_date = "201907150000" 
    # end_date = request.POST['end_date']
    end_date = "201907150100"

    history = History.objects.get(db_conn=DBconn.objects.get(pk=2), 
                                  start_date=start_date,
                                  end_date=end_date)
    if history == None:
        history = History(db_conn=DBconn.objects.get(pk=2), 
                      start_date=start_date,
                      end_date=end_date)
        history.save()

    dbmanager = managers.ConnectionManager(history)
    
    # if connection is right, exeute creating chart.    
    dbmanager.exeute()
    # else return alert Wrong wanning.
    serializer = HistorySerializer(history)
    return Response(serializer.data)

