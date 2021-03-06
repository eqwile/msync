# -*- coding: utf-8 -*-
from mock import Mock
from msync.queryset import (QSPk, QSUpdate, QSUpdateParent, QSUpdateDependentField, QSClear, QSCreate,
                            QSDelete, QSBase)
from .utils import NP, DbSetup


class TestQSPk(DbSetup):
    def test_pass_instance_and_no_sfield(self):
        assert QSPk(sync_cls=self.sync_cls, instance=NP(self.model, id=1)).get_path() == {'id': 1}

    def test_pass_instance_and_sfield(self):
        pk_path = QSPk(sync_cls=self.sync_cls, instance=NP(self.qux, id=1),
                       sfield=self.sync_cls.fk_field)
        assert pk_path.get_path() == {'fk_field__id': 1}

    def test_pass_pk_and_no_instance(self):
        assert QSPk(sync_cls=self.sync_cls, pk=4).get_path() == {'id': 4}

    def test_pass_pk_with_sfield(self):
        pk_path = QSPk(sync_cls=self.sync_cls, pk=8, sfield=self.sync_cls.m2m_field).get_path()
        assert pk_path == {'m2m_field__id': 8}

    def test_pass_no_pk_and_no_instance(self):
        pk_path = QSPk(sync_cls=self.sync_cls, sfield=self.sync_cls.emb_field).get_path()
        assert pk_path == {'emb_field': None}

    def test_hash(self):
        npk = lambda i: QSPk(sync_cls=self.sync_cls, pk=i)
        assert hash(npk(4)) == hash(npk(4))
        assert hash(npk(4)) != hash(npk(8))

    def test_eq(self):
        npk = lambda i: QSPk(sync_cls=self.sync_cls, pk=i)
        assert npk(4) == npk(4)
        assert npk(4) != npk(8)


class TestQSUpdate(DbSetup):
    def test_update_list_sfield(self):
        instance = NP(self.qux)
        document = self.qux_sync.create_document(instance)
        path = QSUpdate(sync_cls=self.sync_cls, document=document, sfield=self.sync_cls.fk_field).get_path()
        assert set(path.keys()) == set(['set__fk_field__S__id', 'set__fk_field__S__str_field'])

    def test_update_ebmedded_sfield(self):
        instance = NP(self.egg)
        document = self.egg_sync.create_document(instance)
        path = QSUpdate(sync_cls=self.sync_cls, document=document, sfield=self.sync_cls.emb_field).get_path()
        assert set(path.keys()) == set(['set__emb_field__id', 'set__emb_field__str_field'])


class TestQSUpdateParent(DbSetup):
    def test_update_parent(self):
        instance = NP(self.model)
        document = self.sync_cls.create_document(instance)
        path = QSUpdateParent(sync_cls=self.sync_cls, document=document).get_path()
        assert set(path.keys()) == set(['set__id', 'set__int_field'])


class TestQSUpdateDependentField(DbSetup):
    def test_update_dependent_field(self):
        instance = NP(self.bar)
        path = QSUpdateDependentField(sync_cls=self.sync_cls, instance=instance,
                                      sfield=self.sync_cls.dep_field).get_path()
        assert path == {'set__dep_field': self.sync_cls.dep_field.value_from_source(instance)}


class TestQSClear(DbSetup):
    def test_m2m_clearing(self):
        instance = NP(self.bar)
        path = QSClear(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field, instance=instance).get_path()
        assert path == {'set__m2m_field': []}


class TestQSCreate(DbSetup):
    def test_m2m_adding(self):
        instance = NP(self.bar)
        document = self.bar_sync.create_document(instance)
        path = QSCreate(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field, document=document).get_path()
        assert path == {'push__m2m_field': document}

    def test_m2m_many_adding(self):
        ins1, ins2 = NP(self.bar), NP(self.bar)
        doc1 = self.bar_sync.create_document(ins1)
        doc2 = self.bar_sync.create_document(ins2)
        path = QSCreate(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field,
                        documents=[doc1, doc2]).get_path()
        assert path == {'push_all__m2m_field': [doc1, doc2]}


class TestQSDelete(DbSetup):
    def test_m2m_deleting(self):
        instance = NP(self.bar)
        path = QSDelete(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field, instance=instance).get_path()
        assert path == {'pull__m2m_field': None}

    def test_m2m_many_deleting(self):
        ins1, ins2 = NP(self.bar), NP(self.bar)
        path = QSDelete(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field,
                        pks=[ins1.pk, ins2.pk]).get_path()
        assert path == {'pull_all__m2m_field__id': [ins1.pk, ins2.pk]}

    def test_m2m_deleting2(self):
        path = QSDelete(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field, pk=15).get_path()
        assert path == {'pull__m2m_field__id': 15}

    def test_emb_delete(self):
        path = QSDelete(sync_cls=self.sync_cls, sfield=self.sync_cls.emb_field, pk=16).get_path()
        assert path == {'unset__emb_field': None}


class TestQSBase(DbSetup):
    def test_union_dependent_fields(self):
        ins = NP(self.bar)
        qs1 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field)
        qs2 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field2)

        qs = qs1 | qs2
        assert qs.get_path() == {'set__dep_field': self.sync_cls.dep_field.value_from_source(ins),
                                 'set__dep_field2': self.sync_cls.dep_field2.value_from_source(ins)}

    def test_union_paths_with_same_keys(self):
        path1 = {'key1': 4, 'key2': 8, 'key3': 23}
        path2 = {'key1': 15, 'key2': 16, 'key4': 42}
        qs1, qs2 = QSBase(), QSBase()
        qs1.get_path = Mock(return_value=path1)
        qs2.get_path = Mock(return_value=path2)

        qs = qs1 | qs2
        assert qs.get_path() == {'key1': 15, 'key2': 16, 'key3': 23, 'key4': 42}
