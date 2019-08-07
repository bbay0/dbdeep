from django.db import models
from dbconn.models import History


class DBInfoReport(models.Model):

    history = models.ForeignKey(History, on_delete=models.CASCADE)
    ftype = models.CharField(max_length=200)
    name = models.CharField(max_length=200) 
    value = models.CharField(max_length=200)
    note = models.CharField(max_length=200)


class MajorTop10Report(models.Model):

    history = models.ForeignKey(History, on_delete=models.CASCADE)
    snap_time = models.CharField(max_length=50)
    value_per_sec_1 = models.CharField(max_length=50)
    value_per_sec_2 = models.CharField(max_length=50)
    value_per_sec_3 = models.CharField(max_length=50)
    value_per_sec_4 = models.CharField(max_length=50)
    value_per_sec_5 = models.CharField(max_length=50)
    value_per_sec_6 = models.CharField(max_length=50)
    value_per_sec_7 = models.CharField(max_length=50)
    value_per_sec_8 = models.CharField(max_length=50)
    value_per_sec_9 = models.CharField(max_length=50)
    value_per_sec_10 = models.CharField(max_length=50)
    value_per_sec_etc = models.CharField(max_length=50)
    value_diff_1 = models.CharField(max_length=50)
    value_diff_2 = models.CharField(max_length=50)
    value_diff_3 = models.CharField(max_length=50)
    value_diff_4 = models.CharField(max_length=50)
    value_diff_5 = models.CharField(max_length=50)
    value_diff_6 = models.CharField(max_length=50)
    value_diff_7 = models.CharField(max_length=50)
    value_diff_8 = models.CharField(max_length=50)
    value_diff_9 = models.CharField(max_length=50)
    value_diff_10 = models.CharField(max_length=50)
    value_diff_etc = models.CharField(max_length=50)


class SQLStatSQLOnlyReport(models.Model):

    history = models.ForeignKey(History, on_delete=models.CASCADE)
    viewtype = models.CharField(max_length=50)
    sql_id = models.CharField(max_length=50)
    optimizer_mode = models.CharField(max_length=50)
    module = models.CharField(max_length=50)
    executions = models.CharField(max_length=50)
    fetches = models.CharField(max_length=50)
    sorts = models.CharField(max_length=50)
    buffer_gets = models.CharField(max_length=50)
    disk_reads = models.CharField(max_length=50)
    rows_processed = models.CharField(max_length=50)
    cpu_time = models.CharField(max_length=50)
    elapsed_time = models.CharField(max_length=50)
    buf_exec = models.CharField(max_length=50)
    disk_exec = models.CharField(max_length=50)
    rows_exec = models.CharField(max_length=50)
    cpu_exec = models.CharField(max_length=50)
    elap_exec = models.CharField(max_length=50)
    iowait = models.CharField(max_length=50)
    clwait = models.CharField(max_length=50)
    apwait = models.CharField(max_length=50)
    ccwait = models.CharField(max_length=50)
    rnum = models.CharField(max_length=50)
    sql_text = models.TextField()
    sql_plan = models.TextField()
    plan_cost1 = models.CharField(max_length=50)
    plan_cost2 = models.CharField(max_length=50)
    bind_value = models.CharField(max_length=50)
    sql_profile = models.CharField(max_length=50)
