# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.db import models
from mongoengine import document, fields as mfields
from msync import fields as sfields
from .utils import to_dict, DefaultQuerySet


class DocumentSchemeFactory(object):

    def __init__(self, name, meta):
        self.name = name
        self.meta = meta

    def create(self):
        mfields = self.get_mfields()
        mfields.update(self.get_qs_managers())
        mfields.update({
            'meta': self.get_meta(),
            'to_dict': to_dict,
            '__module__': self.__module__
        })
        document_cls = type(self.name, self.meta.get_document_bases(), mfields)

        # since pickle wants the class in a global it can have it
        globals()[self.name] = document_cls
        return self.exclude_mfields(document_cls)

    def get_mfields(self):
        return {sfield.name: sfield.get_mfield() for sfield in self.meta.own_sfields}

    def exclude_mfields(self, document_cls):
        exclude = self.meta.exclude
        for name in exclude:
            del document_cls._fields[name]
            del document_cls._db_field_map[name]
        document_cls._fields_ordered = tuple(name for name in document_cls._fields_ordered
                                             if name not in exclude)
        return document_cls

    def get_meta(self):
        m = {'queryset_class': DefaultQuerySet}
        settings = self.meta.collection_settings
        m.update(settings)
        return m

    def get_qs_managers(self):
        return self.meta._qs_managers


class DocumentFieldFactory(object):
    field_mapping = {
        models.AutoField: mfields.IntField,
        models.FloatField: mfields.FloatField,
        models.IntegerField: mfields.IntField,
        models.PositiveIntegerField: mfields.IntField,
        models.SmallIntegerField: mfields.IntField,
        models.PositiveSmallIntegerField: mfields.IntField,
        models.DateTimeField: mfields.DateTimeField,
        models.DateField: mfields.DateTimeField,
        models.TimeField: mfields.DateTimeField,
        models.DecimalField: mfields.DecimalField,
        models.EmailField: mfields.EmailField,
        models.CharField: mfields.StringField,
        models.URLField: mfields.StringField,
        models.SlugField: mfields.StringField,
        models.TextField: mfields.StringField,
        models.CommaSeparatedIntegerField: mfields.StringField,
        models.BooleanField: mfields.BooleanField,
        models.NullBooleanField: mfields.BooleanField,
        models.FileField: mfields.FileField,
        models.ImageField: mfields.ImageField,
    }

    def create(self, field):
        return self.field_mapping.get(field.__class__)


class SyncFieldFactory(object):
    def __init__(self, sync_cls):
        self.mfield_factory = DocumentFieldFactory()
        self.sync_cls = sync_cls

    def create(self, field, **kwargs):
        mfield = self.mfield_factory.create(field)
        if mfield is not None:
            sync_kwargs = {'mfield': mfield(), 'source': field.name, 'primary': field.primary_key,
                           'name': field.name, 'parent_sync_cls': self.sync_cls}
            sync_kwargs.update(kwargs)
            return sfields.SyncField(**sync_kwargs)
        raise TypeError(
            'I don\'t know how to create sync field from %s.%s. Go and create yourself!' % (field.model, field)
        )


class DocumentFactory(object):
    def __init__(self, sync_cls):
        self.sync_cls = sync_cls
        self.meta = sync_cls._meta

    def create(self, instance, with_embedded=False):
        field_values = self.get_field_values_from_sources(instance, self.meta.sfields, with_embedded=with_embedded)
        return self.meta.document(**field_values)

    def bulk_create(self, instances):
        documents = {}
        if not instances:
            return documents
        
        value_dicts = {sfield: sfield.values_from_source(instances) for sfield in self.meta.sfields}
        for instance in instances:
            field_values = {}
            for sfield in self.meta.sfields:
                value = value_dicts[sfield].get(instance)
                if value is not None and sfield.is_nested() and isinstance(value, dict):
                    value = value.values()
                field_values[sfield.name] = value
            documents[instance] = self.meta.document(**field_values)
        return documents

    @classmethod
    def get_field_values_from_sources(cls, instance, sfields, with_embedded=False):
        field_values = {}
        for sfield in sfields:
            if not sfield.is_nested() or with_embedded:
                value = sfield.value_from_source(instance, with_embedded=with_embedded)
                field_values[sfield.name] = value
        return field_values
