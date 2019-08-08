from django.db import models
from dbconn.models import History


class DBInfoReport(models.Model):

    history = models.ForeignKey(History, on_delete=models.CASCADE)
    ftype = models.CharField(max_length=200, null=True)
    name = models.CharField(max_length=200, null=True) 
    value = models.CharField(max_length=200, null=True)
    note = models.CharField(max_length=200, null=True)


class MajorTop10Report(models.Model):

    history = models.ForeignKey(History, on_delete=models.CASCADE)
    snap_time = models.CharField(max_length=50, null=True)
    value_per_sec_1 = models.CharField(max_length=50, null=True)
    value_per_sec_2 = models.CharField(max_length=50, null=True)
    value_per_sec_3 = models.CharField(max_length=50, null=True)
    value_per_sec_4 = models.CharField(max_length=50, null=True)
    value_per_sec_5 = models.CharField(max_length=50, null=True)
    value_per_sec_6 = models.CharField(max_length=50, null=True)
    value_per_sec_7 = models.CharField(max_length=50, null=True)
    value_per_sec_8 = models.CharField(max_length=50, null=True)
    value_per_sec_9 = models.CharField(max_length=50, null=True)
    value_per_sec_10 = models.CharField(max_length=50, null=True)
    value_per_sec_etc = models.CharField(max_length=50, null=True)
    value_diff_1 = models.CharField(max_length=50, null=True)
    value_diff_2 = models.CharField(max_length=50, null=True)
    value_diff_3 = models.CharField(max_length=50, null=True)
    value_diff_4 = models.CharField(max_length=50, null=True)
    value_diff_5 = models.CharField(max_length=50, null=True)
    value_diff_6 = models.CharField(max_length=50, null=True)
    value_diff_7 = models.CharField(max_length=50, null=True)
    value_diff_8 = models.CharField(max_length=50, null=True)
    value_diff_9 = models.CharField(max_length=50, null=True)
    value_diff_10 = models.CharField(max_length=50, null=True)
    value_diff_etc = models.CharField(max_length=50, null=True)


class SQLStatSQLOnlyReport(models.Model):

    history = models.ForeignKey(History, on_delete=models.CASCADE)
    viewtype = models.CharField(max_length=50, null=True)
    sql_id = models.CharField(max_length=50, null=True)
    optimizer_mode = models.CharField(max_length=50, null=True)
    module = models.CharField(max_length=50, null=True)
    executions = models.CharField(max_length=50, null=True)
    fetches = models.CharField(max_length=50, null=True)
    sorts = models.CharField(max_length=50, null=True)
    buffer_gets = models.CharField(max_length=50, null=True)
    disk_reads = models.CharField(max_length=50, null=True)
    rows_processed = models.CharField(max_length=50, null=True)
    cpu_time = models.CharField(max_length=50, null=True)
    elapsed_time = models.CharField(max_length=50, null=True)
    buf_exec = models.CharField(max_length=50, null=True)
    disk_exec = models.CharField(max_length=50, null=True)
    rows_exec = models.CharField(max_length=50, null=True)
    cpu_exec = models.CharField(max_length=50, null=True)
    elap_exec = models.CharField(max_length=50, null=True)
    iowait = models.CharField(max_length=50, null=True)
    clwait = models.CharField(max_length=50, null=True)
    apwait = models.CharField(max_length=50, null=True)
    ccwait = models.CharField(max_length=50, null=True)
    rnum = models.CharField(max_length=50, null=True)
    sql_text = models.TextField(null=True)
    sql_plan = models.TextField(null=True)
    plan_cost1 = models.CharField(max_length=50, null=True)
    plan_cost2 = models.CharField(max_length=50, null=True)
    bind_value = models.CharField(max_length=50, null=True)
    sql_profile = models.CharField(max_length=50, null=True)
