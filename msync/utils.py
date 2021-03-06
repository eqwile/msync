# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import time
import types
import collections
import six
import logging
from contextlib import contextmanager
from django.core.paginator import Paginator
from bson import ObjectId
from mongoengine.queryset import QuerySet, queryset_manager as qm


logger = logging.getLogger(__name__)


def Tree():
    return collections.defaultdict(Tree)


def isfunc(obj):
    return isinstance(obj, (types.FunctionType, types.MethodType))


def islist(obj):
    return isinstance(obj, (list, tuple))


def to_dict(document):
    """
    Удаляет ObjectId и _cls поля и переводит документ в словарь
    """
    remove_fields = ['_cls']
    m = document.to_mongo()
    return {field: m[field] for field in m
            if field not in remove_fields and not isinstance(m[field], ObjectId)}


def to_dicts(documents):
    return [document.to_dict() for document in documents]


class DefaultQuerySet(QuerySet):
    """
    Является QuerySet'ом по умолчанию у сгенерированных документов
    для sync классов
    """
    def to_dicts(self):
        return [document.to_dict() for document in self]


def get_from_source(instance, source_str):
    """Используется в полях для работы с source параметром"""
    source_parts = source_str.split('.')
    source = instance
    for part in source_parts:
        source = apply_source(getattr(source, part, None))
        if source is None:
            return
    return source


def apply_source(source):
    if isfunc(source):
        return source()
    else:
        return source


def _qs_manager_contribute(self, sync_cls, name):
    sync_cls._meta.add_qs_manager(name, self)


def queryset_manager(func):
    """
    Добавляет кастомный менеджер в sync класс, который потом определяется
    в mongoengine документе
    """
    manager = qm(func)
    manager.contribute_to_class = types.MethodType(_qs_manager_contribute, manager)
    return manager


def do_bulk_insert_of_sync_cls(sync_cls, per_page=1000):
    """
    Добавляет в монгу инстансы модельки sync_cls._meta.model порциями
    в per_page штук за раз
    """
    model = sync_cls._meta.model
    document = sync_cls._meta.document

    p = Paginator(model.objects.all(), per_page)
    for i in p.page_range:
        page = p.page(i)
        documents = sync_cls.bulk_create_documents(page.object_list)
        if documents:
            print('%s: %s' % (model, len(documents)))
            document.objects.insert(documents.values())


def with_disabled_msync(f):
    """Используется в тестах для отключения обновления монги sync классами"""
    @six.wraps(f)
    def wrapped(*args, **kwargs):
        with disabled_msync():
            return f(*args, **kwargs)
    return wrapped


@contextmanager
def disabled_msync():
    """Используется в тестах для отключения обновления монги sync классами"""
    from .batches import BatchQuery, BatchTask
    old_bq, old_bt = BatchQuery.run, BatchTask.run
    new_run = lambda self: None
    BatchQuery.run = new_run
    BatchTask.run = new_run
    yield
    BatchQuery.run = old_bq
    BatchTask.run = old_bt


@contextmanager
def measure_time():
    start = time.time()
    yield
    end = time.time()
    logger.info("--- %s seconds ---" % (end - start))
