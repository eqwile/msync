# -*- coding: utf-8 -*-
"""
Существует три типа полей:
    - Простые поля (simple). Обычно используются для синхронизации полей модельки.
      Пример:
          field = SyncField(mfield=IntField(), source='model_field')

    - Встроенные, вложенный (embedded). Позволяют встраивать объекты в схемы документов и
      принимают в качестве аргумента класс EmbeddedSync для задания схемы этого объекта.
      Пример:
          field = EmbeddedField(FooEmbeddedSync, source='foo')

    - Зависимые (dependent). Принимают модельку от которой зависит значение поля,
      поэтому происходит подключение сигналов. Примером использования являются счетчики.
      Пример:
          field = SyncField(mfield=IntField(), depends_on=FooModel, source=source,
                            reverse_rel=reverse_func)
"""
from __future__ import unicode_literals
from collections import defaultdict
import six
from django.db import models
from mongoengine import fields as mfields
from mongoengine.queryset import DO_NOTHING
from .utils import get_from_source


class BaseField(object):
    """Базовый класс для все полей"""

    def __init__(self, mfield, source=None, sync_cls=None, primary=False, reverse_rel=None,
                 depends_on=None, bulk_source=None, is_belongs=None, name=None, parent_sync_cls=None,
                 async=None):
        """
        Инициализирует поле.

        :param mfield: поле mongoengine, которое нужно использовать при генерации
        документа

        :param source: строка или функция, с помощью которой получается значение
        из инстанса джанговской модельки для соответствующего поля

        :param bulk_source: функция, с помощью которой идет получение значений этого
        поля для списка инстансов parent_sync_cls._meta.model. Функция принимает
        список инстансов и возвращает словарь следующего вида: {instance1: document1, ...}

        :param sync_cls: если поле является вложенным (embedded), то это поле содержит
        класс, который определяет структуру вложенного объекта

        :param parent_sync_cls: sync класс, в котором определено это поле. Обычно этот
        параметр добавляется автоматически в функции contribute_to_class

        :param primary: является ли поле primary ключом

        :param reverse_rel: строка или функция, с помощью которой получаются объекты
        этого поля из инстанса parent_sync_cls._meta.model

        :param depends_on: если класс является зависимым, то этот параметр является
        моделькой, от которой зависит это поле и сигналы которой подключаются

        :param is_belongs: функция с сигнатурой: SyncField -> django.db.models.Model -> Bool
        Нужна для фильтрации объектов. Пример:
             liked_users = ListField(..., is_belongs=is_like_belongs, depends_on=Like, ...)
        Здесь определяется поле со списком юзеров, которые лайкнули именно инстанс
        модельки parent_sync_cls._meta.model. Обычно is_belongs используется вместе с
        depens_on параметром. Этот параметр очень полезен для моделек, от которых зависит
        поле, где есть content type поля.

        :param name: название поля, добавляется в contribute_to_class

        :param async: должно ли обновляться это поле ассинхронно
        """
        self.mfield = mfield
        self._source = source
        self._bulk_source = bulk_source
        self.name = name
        self.nested_sync_cls = sync_cls
        self.sync_cls = parent_sync_cls
        self.primary = primary
        self._reverse_rel = reverse_rel
        self.depends_on = depends_on
        self.is_belongs = is_belongs
        self.async = async

    def contribute_to_class(self, sync_cls, name):
        self.sync_cls = sync_cls
        self.name = name
        self._source = name if self._source is None else self._source

        setattr(sync_cls, name, self)
        meta = sync_cls._meta
        meta.add_field(name, self)
        if self.primary:
            meta.setup_pk(self)

    def value_from_source(self, instance, with_embedded=False):
        source = self.get_source()
        if source is not None:
            return source(self, instance)

    def values_from_source(self, instances):
        bulk_source = self.get_bulk_source()
        return bulk_source(instances)

    def get_source(self):
        if hasattr(self._source, '__call__'):
            return self._source
        elif isinstance(self._source, six.string_types):
            return lambda sfield, instance: get_from_source(instance, sfield._source)
        else:
            return None

    def get_bulk_source(self):
        if hasattr(self._bulk_source, '__call__'):
            return self._bulk_source
        elif self._bulk_source is None and self.is_model_sfield():
            return lambda instances: {ins: self.value_from_source(ins, with_embedded=True) for ins in instances}
        else:
            raise TypeError('%s: What the fuck is wrong with bulk source? It\'s your fault!' % self)

    def get_reverse_rel(self):
        if hasattr(self._reverse_rel, '__call__'):
            return self._reverse_rel
        elif isinstance(self._reverse_rel, six.string_types):
            def rev_rel(instance):
                parents = get_from_source(instance, self._reverse_rel)
                try:
                    return list(parents)
                except TypeError:
                    return [parents]
            return rev_rel
        else:
            return lambda instance: []

    def get_mfield(self):
        return self.mfield

    def __hash__(self):
        return hash((self.sync_cls.__module__, self.sync_cls.__name__, self.name))

    def __eq__(self, other):
        return ((self.sync_cls.__module__, self.sync_cls.__name__, self.name) ==
                (other.sync_cls.__module__, other.sync_cls.__name__, other.name))

    def __str__(self):
        return '%s.%s' % (self.sync_cls.__name__, self.name)

    def __getstate__(self):
        # Эти поля обычно являются динамическими и для сериализации не подходят.
        not_pickle = ('_bulk_source', 'mfield', 'is_belongs')
        return dict((k, v) for (k, v) in six.iteritems(self.__dict__) if k not in not_pickle)


class SyncField(BaseField):

    def is_nested(self):
        """
        Поле называется вложенным, если оно хранит вложенные объекты
        """
        return self.nested_sync_cls is not None

    def get_nested_sync_cls(self):
        return self.nested_sync_cls

    def is_depens_on(self):
        """
        Поле называется зависимым, если была определена моделька,
        от которой зависит это поле
        """
        return self.depends_on is not None

    def get_depends_on_model(self):
        return self.depends_on

    def is_model_sfield(self):
        """
        Поле называется простым или полем модельки, если оно не
        является ни вложенным, ни зависимым
        """
        return not self.is_nested() and not self.is_depens_on()

    def is_belongs_to_parent(self, instance):
        if self.is_belongs is not None:
            return self.is_belongs(self, instance)
        return True

    # следующие функции используются при построении запросов к монге
    # через mongoengine. Для каждого типа поля они могут различаться
    def update_query_path(self):
        return self.name

    def remove_operation(self, many=False):
        return 'unset'

    def update_operation(self, new=False, many=False):
        return 'set'


class EmbeddedField(SyncField):
    """
    Используется при определении встроенного объекта в документе:
        class ReviewSync(DocumentSync):
            ...
            user = EmbeddedField(UserSync, ...)
            ...
    """

    def __init__(self, sync_cls, **kwargs):
        """
        :param sync_cls: embedded sync класс, который определяет структуру
        вложенного объекта
        """
        self.meta = sync_cls._meta
        mfield = mfields.EmbeddedDocumentField(self.meta.document)
        super(EmbeddedField, self).__init__(mfield, sync_cls=sync_cls, **kwargs)

    def value_from_source(self, instance, with_embedded=False):
        value = super(EmbeddedField, self).value_from_source(instance)
        return self.get_nested_sync_cls().create_document(value, with_embedded=with_embedded)

    def values_from_source(self, instances):
        value_dict = super(EmbeddedField, self).values_from_source(instances)
        documents = self.get_nested_sync_cls().bulk_create_documents(value_dict.values())
        return {ins: documents[value_dict[ins]] for ins in value_dict}


class ListField(SyncField):
    """
    Используется при определении списковых полей:
        class BookSync(DocumentSync):
            ...
            authors = ListField(sfield=EmbeddedField(AuthorSync), ...)
            ...
    """

    def __init__(self, mfield=None, sfield=None, ordering=None, reverse=False, **kwargs):
        """
        Принимает либо поле из mongoengine, либо sync поле

        :params mfield: поле из mongoengine, который определяет тип данных,
        хранящихся в этом списке

        :params sfield: embedded sync поле, т.е. список содержит объекты
        данной структуры

        :params ordering: нужно ли сортировать список
        :params reverse: в каком порядке сортировать
        """
        nested_mfield, sync_cls = mfield, None
        if mfield is None:
            nested_mfield = sfield.get_mfield()
            sync_cls = sfield.get_nested_sync_cls()

        list_field = self._construct_list_mfield(nested_mfield, ordering=ordering, reverse=reverse)
        super(ListField, self).__init__(list_field, sync_cls=sync_cls, **kwargs)

    def value_from_source(self, instance, with_embedded=False):
        values = super(ListField, self).value_from_source(instance)
        if self.is_nested():
            return [self.get_nested_sync_cls().create_document(value, with_embedded=with_embedded)
                    for value in values]
        return values

    def values_from_source(self, instances):
        value_dict = super(ListField, self).values_from_source(instances)
        if self.is_nested():
            return {ins: self.get_nested_sync_cls().bulk_create_documents(value_dict[ins])
                    for ins in value_dict}
        return value_dict

    def update_query_path(self):
        return '{}__S'.format(self.name) if self.is_nested() else self.name

    def remove_operation(self, many=False):
        return 'pull' if not many else 'pull_all'

    def update_operation(self, new=False, many=False):
        if (not new or self.is_depens_on()) and not many:
            return 'set'
        elif many:
            return 'push_all'
        else:
            return 'push'

    def _construct_list_mfield(self, nested_mfield, ordering=None, reverse=False):
        if ordering:
            return mfields.SortedListField(nested_mfield, ordering=ordering, reverse=reverse)
        return mfields.ListField(nested_mfield)


class ReferenceField(SyncField):
    """
    Для определения ссылочных полей
    """
    def __init__(self, sync_cls, reverse_delete_rule=DO_NOTHING, **kwargs):
        self.ref_sync_cls = sync_cls
        mfield = mfields.ReferenceField(sync_cls._meta.document, reverse_delete_rule=reverse_delete_rule)
        super(ReferenceField, self).__init__(mfield=mfield, **kwargs)

    # FIXME: этой какой-то хак и хз где именно используется
    def is_model_sfield(self):
        return False


class EmbeddedForeignField(EmbeddedField):
    """
    Обычно для синхронизации полей ForeignKey параметр bulk_source один и тот же,
    поэтому выделил в отдельный класс
    """

    def __init__(self, sync_cls, bulk_source=None, *args, **kwargs):
        bs = bulk_source if bulk_source is not None else self._bulk_foreign_key
        self.__model = sync_cls._meta.model
        super(EmbeddedForeignField, self).__init__(sync_cls, bulk_source=bs, *args, **kwargs)

    def _bulk_foreign_key(self, instances):
        ins_dict = {ins: getattr(ins, '%s_id' % self.name) for ins in instances}
        objects = self.__model.objects.in_bulk(ins_dict.values())
        return {ins: objects.get(ins_dict[ins]) for ins in instances}


class ListOfEmbeddedForeignRelatedObjectsField(ListField):
    def __init__(self, sync_cls, sfield=None, bulk_source=None, *args, **kwargs):
        self.__model = sync_cls._meta.model
        self.__fk_field_name = None
        bs = bulk_source if bulk_source is not None else self._bulk_foreign_related_objects
        sf = sfield if sfield is not None else EmbeddedField(sync_cls)
        super(ListOfEmbeddedForeignRelatedObjectsField, self).__init__(sfield=sf, bulk_source=bs, *args, **kwargs)

    def _bulk_foreign_related_objects(self, instances):
        ins_dict = {ins.id: ins for ins in instances}
        fk_name = self._get_fk_field_name()
        objects = self.__model.objects.filter(**{'%s__in' % fk_name: ins_dict.keys()})
        res = defaultdict(list)
        for obj in objects:
            res[ins_dict[getattr(obj, fk_name)]].append(obj)
        return res

    def _get_fk_field_name(self):
        if self.__fk_field_name is None:
            self.__fk_field_name = next(
                field.get_attname()
                for field in self.__model._meta.fields
                if isinstance(field, models.ForeignKey) and field.related_field.model == self.sync_cls._meta.model
            )
        return self.__fk_field_name


class MongoSyncField(SyncField):
    mfield_cls = None

    def __init__(self, *args, **kwargs):
        super(MongoSyncField, self).__init__(mfield=self.mfield_cls(), *args, **kwargs)


class IntField(MongoSyncField):
    mfield_cls = mfields.IntField


class FloatField(MongoSyncField):
    mfield_cls = mfields.FloatField


class DecimalField(MongoSyncField):
    mfield_cls = mfields.DecimalField


class StringField(MongoSyncField):
    mfield_cls = mfields.StringField


class DictField(MongoSyncField):
    mfield_cls = mfields.DictField


class DateTimeField(MongoSyncField):
    mfield_cls = mfields.DateTimeField


class EmailField(MongoSyncField):
    mfield_cls = mfields.EmailField


class URLField(MongoSyncField):
    mfield_cls = mfields.URLField


class BooleanField(MongoSyncField):
    mfield_cls = mfields.BooleanField


class FileField(MongoSyncField):
    mfield_cls = mfields.FileField


class ImageField(MongoSyncField):
    mfield_cls = mfields.ImageField
