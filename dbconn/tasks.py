from config.celery import app
from time import sleep


@app.task
def get_db_report():
    for i in range(100):
        sleep(1)
        print(i)
    return