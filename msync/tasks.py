# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from celery import task


@task.task()
def sync_task(parent_sync_cls, tasks):
    from .batches import BatchQuery

    with BatchQuery(parent_sync_cls) as b:
        for t in tasks:
            t(b)
