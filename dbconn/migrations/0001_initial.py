# Generated by Django 2.1.11 on 2019-08-06 05:40

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='DBconn',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('username', models.CharField(max_length=50)),
                ('password', models.CharField(max_length=50)),
                ('server_ip', models.CharField(max_length=50)),
                ('port', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='History',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('start_date', models.CharField(max_length=50)),
                ('end_date', models.CharField(max_length=50)),
                ('instance_number', models.IntegerField()),
                ('db_conn', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='dbconn.DBconn')),
            ],
        ),
    ]
