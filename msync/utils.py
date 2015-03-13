# -*- coding: utf-8 -*-
import types
import collections
from django.core.paginator import Paginator
from bson import ObjectId
from mongoengine.queryset import QuerySet, QuerySetManager, queryset_manager as qm


def Tree():
    return collections.defaultdict(Tree)


def isfunc(obj):
    return isinstance(obj, (types.FunctionType, types.MethodType))


def islist(obj):
    return isinstance(obj, (list, tuple))


def to_dict(document):
    remove_fields = ['_cls']
    m = document.to_mongo()
    return {field: m[field] for field in m
            if field not in remove_fields and not isinstance(m[field], ObjectId)}


def to_dicts(documents):
    return [document.to_dict() for document in documents]


class DefaultQuerySet(QuerySet):
    def to_dicts(self):
        return [document.to_dict() for document in self]


def get_from_source(instance, source_str):
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
    manager = qm(func)
    manager.contribute_to_class = types.MethodType(_qs_manager_contribute, manager)
    return manager


def do_bulk_insert_of_sync_cls(sync_cls, per_page=1000):
    model = sync_cls._meta.model
    document = sync_cls._meta.document

    p = Paginator(model.objects.all(), per_page)
    for i in p.page_range:
        page = p.page(i)
        documents = sync_cls.bulk_create_documents(page.object_list)
        if documents:
            print '%s: %s' % (model, len(documents))
            document.objects.insert(documents.values())
