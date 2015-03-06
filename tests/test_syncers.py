import pytest
from django.db import models
from mongoengine import fields as mfields
from msync import fields as sfields
from msync.syncers import DocumentSync, EmbeddedSync


class BarModel(models.Model):
    quux = models.IntegerField()


class FooModel(models.Model):
    text = models.CharField()
    boo = models.ForeignKey(BarModel)


class TestFieldsDocumentSync:

    def setup(self):
        class BarSync(EmbeddedSync):
            class Meta:
                fields = ('id', 'quux')
                model = BarModel

        class FooSync(DocumentSync):
            boo = sfields.EmbeddedForeignField(BarSync)
            class Meta:
                fields = ('id', 'text', 'boo')
                model = FooModel
        self.sync_cls = FooSync
        self.embedded_sync_cls = BarSync

    def test_meta_model(self):
        assert self.sync_cls._meta.model == FooModel

    def test_meta_sync_cls(self):
        assert self.sync_cls._meta.sync_cls == self.sync_cls
    
        
