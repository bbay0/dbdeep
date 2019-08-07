from .models import DBconn, History 
from rest_framework import serializers

class DBconnSerializer(serializers.ModelSerializer):

    class Meta:
        model = DBconn
        fields = ['id', 'username', 'password', 'server_ip', 'port']


class HistorySerializer(serializers.ModelSerializer):
    
    db_conn = serializers.StringRelatedField()
    
    class Meta:
        model = History
        fields = ['id', 'db_conn' ,'start_date', 'end_date', 'instance_number']
