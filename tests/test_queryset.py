from mock import Mock, MagicMock, call
from msync.queryset import (QSPk, QSUpdate, QSUpdateParent, QSUpdateDependentField, QSClear, QSCreate,
                            QSDelete, BatchQuery, QSBase)
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
        assert path == {'pull__m2m_field__id': instance.id}

    def test_m2m_many_deleting(self):
        ins1, ins2 = NP(self.bar), NP(self.bar)
        path = QSDelete(sync_cls=self.sync_cls, sfield=self.sync_cls.m2m_field,
                        pks=[ins1.pk, ins2.pk]).get_path()
        assert path == {'pull_all__m2m_field__id': [ins1.pk, ins2.pk]}

    def test_m2m_deleting(self):
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


class TestBatchQuery(DbSetup):
    def setup(self):
        super(TestBatchQuery, self).setup()
        self.sync_cls._meta.document = Mock()
        self.filter_mock = self.sync_cls._meta.document.objects.filter
        self.batch = BatchQuery(self.sync_cls)

    def test_saving_dependent_fields_of_same_parent(self):
        pi = NP(self.model, id=4)
        ins = NP(self.bar)
        qs1 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field)
        qs2 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field2)

        self.batch[pi] = qs1
        self.batch[pi] = qs2

        self.batch.run()
        self.filter_mock.assert_called_once_with(id=4)

    def test_saving_nested_field_with_dependent(self):
        pi = NP(self.model, id=8)
        ins = NP(self.bar, id=15)
        ins_document = self.bar_sync.create_document(ins)

        qs1 = QSUpdate(sync_cls=self.sync_cls, document=ins_document, sfield=self.sync_cls.m2m_field)
        self.batch[(ins, self.sync_cls.m2m_field)] = qs1
        
        qs2 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field)
        self.batch[pi] = qs2

        self.batch.run()
        assert self.filter_mock.call_count == 2
        self.filter_mock.assert_has_calls([call(m2m_field__id=15), call(id=8)], any_order=True)
        

    def test_saving_new_nested_field_with_dependent(self):
        pi = NP(self.model, id=16)
        ins = NP(self.bar, id=23)
        ins_document = self.bar_sync.create_document(ins)

        qs1 = QSUpdate(sync_cls=self.sync_cls, document=ins_document, sfield=self.sync_cls.m2m_field)
        self.batch[pi] = qs1
        
        qs2 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field)
        self.batch[pi] = qs2

        self.batch.run()
        self.filter_mock.assert_called_once_with(id=16)
