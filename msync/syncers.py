# -*- coding: utf-8 -*-
import six
from pprint import pprint
from django.db import models
from mongoengine import document, fields as mfields
from .fields import BaseField
from .options import Options
from .factories import DocumentSchemeFactory, DocumentFactory
from .signals import SignalConnector



class SyncMC(type):
    def __new__(cls, name, bases, attrs):
        ############################################################################################
        # basic checkings
        super_new = super(SyncMC, cls).__new__
        # print '=' * 100
        # print name, bases, attrs

        if name == 'NewBase' and attrs == {}:
            return super_new(cls, name, bases, attrs)

        if name in ('DocumentSync', 'EmbeddedSync', 'DynamicDocumentSync', 'DynamicEmbeddedSync'):
            return super_new(cls, name, bases, attrs)

        parents = [b for b in bases if isinstance(b, SyncMC) and
                   not (b.__name__ == 'NewBase' and b.__mro__ == (b, object))]

        if not parents:
            return super_new(cls, name, bases, attrs)

        ############################################################################################
        # create new class
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})

        ############################################################################################
        # check attributes
        attr_meta = attrs.pop('Meta', None)
        if attr_meta is None:
            raise TypeError('%s: no Meta' % name)

        ############################################################################################
        # create meta
        meta = Options(new_class, attr_meta, bases, document_type=new_class.document_type)
        new_class._meta = meta

        ############################################################################################
        # contribute to class
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)

        meta._add_model_fields()
        ############################################################################################
        # create document scheme
        dsfactory = DocumentSchemeFactory(name, meta)
        meta.document = dsfactory.create()
        # print
        # print 'MMODEL:', meta.document.__bases__
        # pprint(meta.document._fields)

        ############################################################################################
        # create document factory
        new_class._document_factory = DocumentFactory(new_class)

        ############################################################################################
        # connect signals
        new_class.connect_signals()

        ############################################################################################
        # footer
        # print '=' * 100
        return new_class

    def add_to_class(cls, name, value):
        if hasattr(value, 'contribute_to_class'):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)


class SyncBase(six.with_metaclass(SyncMC)):
    document_type = None
    signal_connector_cls = None

    @classmethod
    def create_document(cls, instance, with_embedded=False):
        if cls._meta.pass_filter(instance):
            return cls._document_factory.create(instance, with_embedded=with_embedded)

    @classmethod
    def bulk_create_documents(cls, instances):
        passed_instances = filter(cls._meta.pass_filter, instances)
        return cls._document_factory.bulk_create(passed_instances)

    @classmethod
    def connect_signals(cls):
        if cls.signal_connector_cls is not None and cls._meta.is_need_to_connect_signals():
            signal_connector = cls.signal_connector_cls(cls)
            signal_connector.setup()


class DocumentSync(SyncBase):
    document_type = document.Document
    signal_connector_cls = SignalConnector


class EmbeddedSync(SyncBase):
    document_type = document.EmbeddedDocument


class DynamicDocumentSync(DocumentSync):
    document_type = document.DynamicDocument


class DynamicEmbeddedSync(EmbeddedSync):
    document_type = document.DynamicEmbeddedDocument