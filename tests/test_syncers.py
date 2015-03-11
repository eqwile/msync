import pytest
from mock import Mock, MagicMock
from django.db import models
from mongoengine import fields as mfields
from msync import fields as sfields
from msync.syncers import DocumentSync, EmbeddedSync
from msync.options import Options


# class BarModel(models.Model):
#     quux = models.IntegerField()


# class FooModel(models.Model):
#     text = models.CharField()
#     boo = models.ForeignKey(BarModel)


# class TestFieldsDocumentSync:

#     def setup(self):
#         class BarEmSync(EmbeddedSync):
#             class Meta:
#                 fields = ('id', 'quux')
#                 model = BarModel

#         class FooBarSync(DocumentSync):
#             boo = sfields.EmbeddedForeignField(BarEmSync)
#             class Meta:
#                 fields = ('id', 'text', 'boo')
#                 model = FooModel
#         self.sync_cls = FooBarSync
#         self.embedded_sync_cls = BarEmSync

#     def test_meta_model(self):
#         assert self.sync_cls._meta.model == FooModel

#     def test_meta_sync_cls(self):
#         assert self.sync_cls._meta._sync_cls == self.sync_cls
    

# class TestMetaFieldsAttr:
#     def setup(self):
#         class Foo(models.Model):
#             field1 = models.IntegerField()
#             field2 = models.IntegerField()
#             field3 = models.IntegerField()

#         class FooSync(DocumentSync):
#             class Meta:
#                 fields = ('field1', 'field2')
#                 model = Foo
#         self.sync_cls = FooSync

#     def test_count_of_fields(self):
#         assert len(self.sync_cls._meta._sfields) == 2

#     def test_field3_not_in_sfields(self):
#         assert 'field3' not in self.sync_cls._meta._sfields_dict

#     def test_sfields_eq_to_sfields_dict(self):
#         assert len(self.sync_cls._meta._sfields) == len(self.sync_cls._meta._sfields_dict)



# class TestOptions:
#     def setup(self):
#         self.m_sync_bases = MagicMock()
#         self.m_sync_cls = MagicMock()
#         self.m_meta = MagicMock()
#         self.options = Options(self.m_sync_cls, self.m_meta, self.m_sync_bases)

#     def test_field_normalizing(self):
#         assert (self.options._normalize_meta_fields(['field1', ('field2', 'field3')]) ==
#                 [('field1', 'field1'), ('field2', 'field3')])
