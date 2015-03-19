from celery import task
from msync.queryset import BatchQuery


@task.task()
def sync_task(parent_sync_cls, tasks):
    with BatchQuery(parent_sync_cls) as b:
        for t in tasks:
            t(b)
