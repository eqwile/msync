import pytest
from mock import Mock, MagicMock
from django.db import models
from mongoengine import fields as mfields
from msync import fields as sfields
from msync.syncers import DocumentSync, EmbeddedSync
from msync.options import Options
from .utils import DbSetup


class TestOptions(DbSetup):
    def test_meta_model(self):
        assert self.sync_cls._meta.model == self.model

    def test_meta_sync_cls(self):
        assert self.sync_cls._meta._sync_cls == self.sync_cls

    def test_pk_setup(self):
        assert self.sync_cls._meta._pk_sfield == self.sync_cls.id
        assert self.bar_sync._meta._pk_sfield == self.bar_sync.id
        assert self.qux_sync._meta._pk_sfield == self.qux_sync.id
        assert self.egg_sync._meta._pk_sfield == self.egg_sync.id

    def test_sfields(self):
        assert set(self.sync_cls._meta._sfields_dict.keys()) == set(self.sync_cls._meta.fields)
        assert set(self.egg_sync._meta._sfields_dict.keys()) == set(self.egg_sync._meta.fields)
        assert set(self.bar_sync._meta._sfields_dict.keys()) == set(self.bar_sync._meta.fields)
        assert set(self.qux_sync._meta._sfields_dict.keys()) == set(self.qux_sync._meta.fields)

    def test_qs_managers(self):
        assert len(self.sync_cls._meta._qs_managers) == 1

    def test_collection_settings(self):
        assert self.sync_cls._meta.collection_settings == {'collection': 'foos', 'id_field': 'id'}
