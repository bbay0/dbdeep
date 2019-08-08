from celery import Celery

app = Celery('project',
    broker='amqp://',
    backend='amqp://daeseong:eotjd@localhost:15672//',
    include=['config.tasks']
)