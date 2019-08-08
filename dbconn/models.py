from django.db import models


class DBconn(models.Model):

    username = models.CharField( max_length=50)
    password = models.CharField( max_length=50)
    server_ip = models.CharField( max_length=50)
    port = models.IntegerField()
    sid = models.CharField(default="TEST", max_length=50)

    def __str__(self):
        return f'{self.username} - {self.server_ip}:{self.port}'

        
class History(models.Model):

    db_conn = models.ForeignKey(DBconn, on_delete=models.CASCADE)
    start_date = models.CharField( max_length=50)
    end_date = models.CharField( max_length=50)
    instance_number = models.IntegerField(default=1)

    def __str__(self):
        return f'{self.db_conn} / {self.start_date[:-4]} ~ {self.end_date[:-4]}'

    def get_db_conn(self):
        return self.db_conn