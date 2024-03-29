# Generated by Django 2.1.11 on 2019-08-08 01:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('report', '0002_auto_20190808_1055'),
    ]

    operations = [
        migrations.AlterField(
            model_name='majortop10report',
            name='snap_time',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_1',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_10',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_2',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_3',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_4',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_5',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_6',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_7',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_8',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_9',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_diff_etc',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_1',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_10',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_2',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_3',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_4',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_5',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_6',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_7',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_8',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_9',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='majortop10report',
            name='value_per_sec_etc',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='apwait',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='bind_value',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='buf_exec',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='buffer_gets',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='ccwait',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='clwait',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='cpu_exec',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='cpu_time',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='disk_exec',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='disk_reads',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='elap_exec',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='elapsed_time',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='executions',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='fetches',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='iowait',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='module',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='optimizer_mode',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='plan_cost1',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='plan_cost2',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='rnum',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='rows_exec',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='rows_processed',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='sorts',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='sql_id',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='sql_profile',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='sqlstatsqlonlyreport',
            name='viewtype',
            field=models.CharField(max_length=50, null=True),
        ),
    ]
