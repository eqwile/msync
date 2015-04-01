# -*- coding: utf-8 -*-
from mock import Mock, call
from msync.queryset import QSPk, QSUpdateDependentField, QSUpdate, QSUpdateParent
from msync.batches import BatchQuery
from .utils import NP, DbSetup


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

        self._mock_update_number(0)
        self.batch.run()

        self._get_filter_mock().assert_called_once_with(id=4)
        self.batch._sync_cls.create_document.assert_called_once_with(pi, with_embedded=True)

    def test_saving_nested_field_with_dependent(self):
        pi = NP(self.model, id=8)
        ins = NP(self.bar, id=15)
        ins_document = self.bar_sync.create_document(ins)

        qs1 = QSUpdate(sync_cls=self.sync_cls, document=ins_document, sfield=self.sync_cls.m2m_field)
        self.batch[(ins, self.sync_cls.m2m_field)] = qs1

        qs2 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field)
        self.batch[pi] = qs2

        self._mock_update_number(0)
        self.batch.run()

        assert self._get_filter_mock().call_count == 2
        self._get_filter_mock().assert_has_calls([call(m2m_field__id=15), call(id=8)], any_order=True)
        self.batch._sync_cls.create_document.assert_called_once_with(pi, with_embedded=True)

    def test_saving_new_nested_field_with_dependent(self):
        pi = NP(self.model, id=16)
        ins = NP(self.bar, id=23)
        ins_document = self.bar_sync.create_document(ins)

        qs1 = QSUpdate(sync_cls=self.sync_cls, document=ins_document, sfield=self.sync_cls.m2m_field)
        self.batch[pi] = qs1

        qs2 = QSUpdateDependentField(instance=ins, sync_cls=self.sync_cls, sfield=self.sync_cls.dep_field)
        self.batch[pi] = qs2

        self._mock_update_number(0)
        self.batch.run()
        
        self._get_filter_mock().assert_called_once_with(id=16)
        self.batch._sync_cls.create_document.assert_called_once_with(pi, with_embedded=True)

    def test_saving_parent_model(self):
        pi = NP(self.model, id=42)
        pi_document = self.sync_cls.create_document(pi)
        
        self.batch[pi] = QSUpdateParent(sync_cls=self.sync_cls, document=pi_document)

        self._mock_update_number(0)
        self.batch.run()

        self.batch._sync_cls.create_document.assert_called_once_with(pi, with_embedded=True)

    def test_with_different_pk(self):
        self.batch[NP(self.model, id=4)] = None
        self.batch[NP(self.model, id=8)] = None
        assert len(self.batch._qs_collection) == 2

    def test_with_same_pk(self):
        ins = NP(self.model, id=4)
        self.batch[ins] = None
        self.batch[ins] = None

        pk = QSPk(sync_cls=self.sync_cls, instance=ins)
        assert len(self.batch._qs_collection) == 1 and len(self.batch._qs_collection[pk]) == 2

    def _mock_update_number(self, count):
        self.batch._sync_cls = Mock(**{'_meta.model': self.sync_cls._meta.model})
        update_mock = self._get_update_mock()
        update_mock.return_value = count
        return update_mock

    def _get_filter_mock(self):
        return self.batch._sync_cls._meta.document.objects.filter

    def _get_update_mock(self):
        return self._get_filter_mock().return_value.update
