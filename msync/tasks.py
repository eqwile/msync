from celery import task


@task.task()
def sync_task(parent_sync_cls, tasks):
    from .queryset import BatchQuery

    with BatchQuery(parent_sync_cls) as b:
        for t in tasks:
            t(b)
