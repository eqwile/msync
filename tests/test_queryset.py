from django.db import models
from django_dynamic_fixture import N
from msync.syncers import DocumentSync, EmbeddedSync
from msync import fields as sfields
from msync.queryset import QSPk


class QSSetup(object):
    def setup(self):
        class Bar(models.Model):
            str_field = models.CharField()

        class Foo(models.Model):
            int_field = models.IntegerField()
            m2m_field = models.ManyToManyField(Bar)

        class Qux(models.Model):
            fk_field = models.ForeignKey(Foo)
            str_field = models.CharField()

        class BarSync(EmbeddedSync):
            class Meta:
                model = Bar
                fields = ('id', 'str_field')

        class QuxSync(EmbeddedSync):
            class Meta:
                model = Qux
                fields = ('id', 'str_field')

        class FooSync(DocumentSync):
            m2m_field = sfields.ListField(sfield=sfields.EmbeddedField(BarSync), source='m2m_field.all',
                                          reverse_rel='foo_set.all')
            fk_field = sfields.ListField(sfield=sfields.EmbeddedField(QuxSync), source='qux_set.all',
                                         reverse_rel='foo')

            class Meta:
                model = Foo
                fields = ('id', 'int_field', 'm2m_field')

        self.sync_cls = FooSync
        self.foo_sync = FooSync
        self.bar_sync = BarSync
        self.qux_sync = QuxSync
        self.model = Foo
        self.bar = Bar
        self.foo = Foo
        self.qux = Qux


class TestQSPk(QSSetup):
    def test_pass_instance_and_no_sfield(self):
        assert QSPk(sync_cls=self.sync_cls, instance=N(self.model, id=1)).get_path() == {'id': 1}

    def test_pass_instance_and_sfield(self):
        pk_path = QSPk(sync_cls=self.sync_cls, instance=N(self.model, id=1), sfield=self.sync_cls.fk_field)
        assert pk_path.get_path() == {'fk_field__id': 1}
