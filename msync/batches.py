# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import six
from six.moves import reduce
import time
import operator
from collections import defaultdict
from .queryset import QSPk
from .tasks import sync_task


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
        # print('')
        self.run()

    def run(self):
        for pk_path, qss in six.iteritems(self._qs_collection):
            qs = reduce(operator.or_, qss)
            qs_path = qs.get_path()
            pk_path = dict(pk_path)
            # print('{}.filter({}).update({})'.format(self._sync_cls, pk_path, qs_path))
            start_time = time.time()
            self._sync_cls._meta.document.objects.filter(**pk_path).update(**qs_path)
            # print("--- %s seconds ---" % (time.time() - start_time))

    def __setitem__(self, key, qs):
        key = self._get_key(key)
        self._qs_collection[key].append(qs)

    def _get_key(self, k):
        try:
            ins, sfield = k
        except TypeError:
            ins, sfield = k, None

        pk_path = QSPk(sync_cls=self._sync_cls, instance=ins, sfield=sfield).get_path()
        return frozenset(six.iteritems(pk_path))


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
