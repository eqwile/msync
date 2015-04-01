# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import six
from six.moves import reduce
import logging
import time
import operator
from collections import defaultdict
from .queryset import QSPk
from .tasks import sync_task
from .utils import measure_time


logger = logging.getLogger(__name__)


class BatchQuery(object):
    def __init__(self, sync_cls):
        self._sync_cls = sync_cls
        self._qs_collection = defaultdict(list)

    @property
    def qs_collection(self):
        return self._qs_collection

    def __enter__(self):
        self._qs_collection.clear()
        return self

    def __exit__(self, t, value, traceback):
        self.run()

    def run(self):
        for pk, qss in six.iteritems(self._qs_collection):
            qs = reduce(operator.or_, qss)
            qs_path = qs.get_path()
            pk_path = pk.get_path()

            logger.info('{}.filter({}).update({})'.format(self._sync_cls, pk_path, qs_path))
            with measure_time():
                updated_number = self._sync_cls._meta.document.objects.filter(**pk_path).update(**qs_path)

            if updated_number == 0 and self.is_instance_of_parent(pk.instance):
                logger.warning('%s with path %s is not in mongo. Saving to %s.' % (
                    pk.instance.__class__, pk_path, self._sync_cls))
                self._sync_cls.create_document(pk.instance, with_embedded=True).save()

    def is_instance_of_parent(self, instance):
        model = self._sync_cls._meta.model
        return model is not None and isinstance(instance, model)

    def __setitem__(self, key, qs):
        pk = self._get_pk(key)
        self._qs_collection[pk].append(qs)

    def _get_pk(self, k):
        try:
            ins, sfield = k
        except TypeError:
            ins, sfield = k, None

        return QSPk(sync_cls=self._sync_cls, instance=ins, sfield=sfield)


class BatchTask(object):
    def __init__(self, sync_cls):
        self._sync_cls = sync_cls
        self._async_tasks = []

    def add(self, task):
        self._async_tasks.append(task)

    def __enter__(self):
        del self._async_tasks[:]
        return self

    def __exit__(self, t, value, traceback):
        self.run()

    def run(self):
        if not self._async_tasks:
            return

        sync_task.delay(self._sync_cls, self._async_tasks)
