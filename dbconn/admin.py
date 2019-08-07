from django.contrib import admin
from dbconn.models import DBconn, History

admin.site.register(DBconn)
admin.site.register(History)