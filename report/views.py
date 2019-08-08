from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.views import APIView
from rest_framework.response import Response

from util.decorators import ajax_required
from .models import DBInfoReport, SQLStatSQLOnlyReport, MajorTop10Report

def report(request):
    taplate_name = 'dbconn/report.html'
    return render(request, taplate_name)

@ajax_required
@api_view(['GET'])
def get_dbinfo_report(request, history_id):
    db_info_report_list = DBInfoReport.objects.filter(history__id=history_id)
    ftype = []
    name = []
    value = []
    note = []

    for db_info_report in db_info_report_list:
        ftype.append(db_info_report.ftype)
        name.append(db_info_report.name)
        value.append(db_info_report.value)
        note.append(db_info_report.note)
    
    data = {
        "ftype" : ftype,
        "name" : name,
        "value" : value,
        "note" : note
    }
    return Response(data)

@ajax_required
@api_view(['GET'])
def get_major_top_10_report(request, history_id):
    major_top_10_report_list = MajorTop10Report.objects.filter(history__id=history_id)
    snap_time = []
    value_per_sec_1 = []
    value_per_sec_2 = []
    value_per_sec_3 = []
    value_per_sec_4 = []
    value_per_sec_5 = []
    value_per_sec_6 = []
    value_per_sec_7 = []
    value_per_sec_8 = []
    value_per_sec_9 = []
    value_per_sec_10 = []
    value_per_sec_etc = []
    value_diff_1 = []
    value_diff_2 = []
    value_diff_3 = []
    value_diff_4 = []
    value_diff_5 = []
    value_diff_6 = []
    value_diff_7 = []
    value_diff_8 = []
    value_diff_9 = []
    value_diff_10 = []
    value_diff_etc = []

    for major_top_10_report in major_top_10_report_list:
        snap_time.append(major_top_10_report.snap_time)
        value_per_sec_1.append(major_top_10_report.value_per_sec_1)
        value_per_sec_2.append(major_top_10_report.value_per_sec_2)
        value_per_sec_3.append(major_top_10_report.value_per_sec_3)
        value_per_sec_4.append(major_top_10_report.value_per_sec_4)
        value_per_sec_5.append(major_top_10_report.value_per_sec_5)
        value_per_sec_6.append(major_top_10_report.value_per_sec_6)
        value_per_sec_7.append(major_top_10_report.value_per_sec_7)
        value_per_sec_8.append(major_top_10_report.value_per_sec_8)
        value_per_sec_9.append(major_top_10_report.value_per_sec_9)
        value_per_sec_10.append(major_top_10_report.value_per_sec_10)
        value_per_sec_etc.append(major_top_10_report.value_per_sec_etc)
        value_diff_1.append(major_top_10_report.value_diff_1)
        value_diff_2.append(major_top_10_report.value_diff_2)
        value_diff_3.append(major_top_10_report.value_diff_3)
        value_diff_4.append(major_top_10_report.value_diff_4)
        value_diff_5.append(major_top_10_report.value_diff_5)
        value_diff_6.append(major_top_10_report.value_diff_6)
        value_diff_7.append(major_top_10_report.value_diff_7)
        value_diff_8.append(major_top_10_report.value_diff_8)
        value_diff_9.append(major_top_10_report.value_diff_9)
        value_diff_10.append(major_top_10_report.value_diff_10)
        value_diff_etc.append(major_top_10_report.value_diff_etc)
    
    data = {
        "snap_time" : snap_time,
        "value_per_sec_1" : value_per_sec_1,
        "value_per_sec_2" : value_per_sec_2,
        "value_per_sec_3" : value_per_sec_3,
        "value_per_sec_4" : value_per_sec_4,
        "value_per_sec_5" : value_per_sec_5,
        "value_per_sec_6" : value_per_sec_6,
        "value_per_sec_7" : value_per_sec_7,
        "value_per_sec_8" : value_per_sec_8,
        "value_per_sec_9" : value_per_sec_9,
        "value_per_sec_10" : value_per_sec_10,
        "value_per_sec_etc" : value_per_sec_etc,
        "value_diff_1" : value_diff_1,
        "value_diff_2" : value_diff_2,
        "value_diff_3" : value_diff_3,
        "value_diff_4" : value_diff_4,
        "value_diff_5" : value_diff_5,
        "value_diff_6" : value_diff_6,
        "value_diff_7" : value_diff_7,
        "value_diff_8" : value_diff_8,
        "value_diff_9" : value_diff_9,
        "value_diff_10" : value_diff_10,
        "value_diff_etc" : value_diff_etc
    }
    return Response(data)

@ajax_required
@api_view(['GET'])
def get_sql_stat_sql_only_report(request, history_id):
    sql_stat_sql_only_list = SQLStatSQLOnlyReport.objects.filter(history__id=history_id)
    viewtype = [] 
    sql_id = [] 
    optimizer_mode = [] 
    module = [] 
    executions = [] 
    fetches = [] 
    sorts = [] 
    buffer_gets = [] 
    disk_reads = [] 
    rows_processed = [] 
    cpu_time = [] 
    elapsed_time = [] 
    buf_exec = [] 
    disk_exec = [] 
    rows_exec = [] 
    cpu_exec = [] 
    elap_exec = [] 
    iowait = [] 
    clwait = [] 
    apwait = [] 
    ccwait = [] 
    rnum = [] 
    sql_text = [] 
    sql_plan = [] 
    plan_cost1 = [] 
    plan_cost2 = [] 
    bind_value = [] 
    sql_profile = [] 

    for sql_stat_sql_only in sql_stat_sql_only_list:
        viewtype.append(sql_stat_sql_only.viewtype) 
        sql_id.append(sql_stat_sql_only.sql_id) 
        optimizer_mode.append(sql_stat_sql_only.optimizer_mode) 
        module.append(sql_stat_sql_only.module) 
        executions.append(sql_stat_sql_only.executions) 
        fetches.append(sql_stat_sql_only.fetches) 
        sorts.append(sql_stat_sql_only.sorts) 
        buffer_gets.append(sql_stat_sql_only.buffer_gets) 
        disk_reads.append(sql_stat_sql_only.disk_reads) 
        rows_processed.append(sql_stat_sql_only.rows_processed) 
        cpu_time.append(sql_stat_sql_only.cpu_time) 
        elapsed_time.append(sql_stat_sql_only.elapsed_time) 
        buf_exec.append(sql_stat_sql_only.buf_exec) 
        disk_exec.append(sql_stat_sql_only.disk_exec) 
        rows_exec.append(sql_stat_sql_only.rows_exec) 
        cpu_exec.append(sql_stat_sql_only.cpu_exec) 
        elap_exec.append(sql_stat_sql_only.elap_exec) 
        iowait.append(sql_stat_sql_only.iowait) 
        clwait.append(sql_stat_sql_only.clwait) 
        apwait.append(sql_stat_sql_only.apwait) 
        ccwait.append(sql_stat_sql_only.ccwait) 
        rnum.append(sql_stat_sql_only.rnum) 
        sql_text.append(sql_stat_sql_only.sql_text) 
        sql_plan.append(sql_stat_sql_only.sql_plan) 
        plan_cost1.append(sql_stat_sql_only.plan_cost1) 
        plan_cost2.append(sql_stat_sql_only.plan_cost2) 
        bind_value.append(sql_stat_sql_only.bind_value) 
        sql_profile.append(sql_stat_sql_only.sql_profile) 
    
    data = {
        "viewtype" : viewtype, 
        "sql_id" : sql_id, 
        "optimizer_mode" : optimizer_mode, 
        "module" : module, 
        "executions" : executions, 
        "fetches" : fetches, 
        "sorts" : sorts, 
        "buffer_gets" : buffer_gets, 
        "disk_reads" : disk_reads, 
        "rows_processed" : rows_processed, 
        "cpu_time" : cpu_time, 
        "elapsed_time" : elapsed_time, 
        "buf_exec" : buf_exec, 
        "disk_exec" : disk_exec, 
        "rows_exec" : rows_exec, 
        "cpu_exec" : cpu_exec, 
        "elap_exec" : elap_exec, 
        "iowait" : iowait, 
        "clwait" : clwait, 
        "apwait" : apwait, 
        "ccwait" : ccwait, 
        "rnum" : rnum, 
        "sql_text" : sql_text, 
        "sql_plan" : sql_plan, 
        "plan_cost1" : plan_cost1, 
        "plan_cost2" : plan_cost2, 
        "bind_value" : bind_value, 
        "sql_profile" : sql_profile
    }
    return Response(data)