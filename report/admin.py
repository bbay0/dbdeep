from django.contrib import admin
from report.models import ( DBInfoReport,
                            MajorTop10Report,
                            SQLStatSQLOnlyReport 
                          )

admin.site.register(DBInfoReport)
admin.site.register(MajorTop10Report)
admin.site.register(SQLStatSQLOnlyReport)