# -*- coding: utf-8 -*-
from collections import defaultdict
from django.db import models
from mongoengine import fields as mfields
from mongoengine.queryset import DO_NOTHING
from .utils import get_from_source


class BaseField(object):
    def __init__(self, mfield, source=None, sync_cls=None, primary=False, reverse_rel=None,
                 depends_on=None, bulk_source=None, is_belongs=None, name=None, parent_sync_cls=None):
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
        elif isinstance(self._source, basestring):
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
        elif isinstance(self._reverse_rel, basestring):
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

    def __str__(self):
        return '%s.%s' % (self.sync_cls.__name__, self.name)


class SyncField(BaseField):

    def is_nested(self):
        """Nested means it contains embedded syncs"""
        return self.nested_sync_cls is not None

    def get_nested_sync_cls(self):
        return self.nested_sync_cls

    def is_depens_on(self):
        return self.depends_on is not None

    def get_depends_on_model(self):
        return self.depends_on

    def is_model_sfield(self):
        return not self.is_nested() and not self.is_depens_on()

    def is_belongs_to_parent(self, instance):
        if self.is_belongs is not None:
            return self.is_belongs(self, instance)
        return True

    def update_query_path(self):
        return self.name

    def remove_operation(self, many=False):
        return 'unset'

    def update_operation(self, new=False, many=False):
        return 'set'


class EmbeddedField(SyncField):
    def __init__(self, sync_cls, **kwargs):
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
    def __init__(self, mfield=None, sfield=None, ordering=None, reverse=False, **kwargs):
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
    def __init__(self, sync_cls, reverse_delete_rule=DO_NOTHING, **kwargs):
        self.ref_sync_cls = sync_cls
        mfield = mfields.ReferenceField(sync_cls._meta.document, reverse_delete_rule=reverse_delete_rule)
        super(ReferenceField, self).__init__(mfield=mfield, **kwargs)

    def is_model_sfield(self):
        return False


class EmbeddedForeignField(EmbeddedField):
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
        super(ListEmbeddedForeignRelatedField, self).__init__(sfield=sf, bulk_source=bs, *args, **kwargs)

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
