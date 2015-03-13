from django.db import models
from django_dynamic_fixture import N
from mongoengine import fields as mfields
from msync.syncers import DocumentSync, EmbeddedSync
from msync import fields as sfields
from msync.utils import queryset_manager


NP = lambda *args, **kwargs: N(persist_dependencies=False, *args, **kwargs)


class DbSetup(object):
    def setup(self):
        class Bar(models.Model):
            str_field = models.CharField()

        class Egg(models.Model):
            str_field = models.CharField()

        class Foo(models.Model):
            int_field = models.IntegerField()
            m2m_field = models.ManyToManyField(Bar)
            emb_field = models.ForeignKey(Egg)
            not_included_field = models.CharField()

        class Qux(models.Model):
            fk_field = models.ForeignKey(Foo, blank=True, null=True)
            str_field = models.CharField()

        class BarSync(EmbeddedSync):
            class Meta:
                model = Bar
                fields = ('id', 'str_field')

        class QuxSync(EmbeddedSync):
            class Meta:
                model = Qux
                fields = ('id', 'str_field')

        class EggSync(EmbeddedSync):
            class Meta:
                model = Egg
                fields = ('id', 'str_field')

        class FooSync(DocumentSync):
            m2m_field = sfields.ListField(sfield=sfields.EmbeddedField(BarSync), source='m2m_field.all',
                                          reverse_rel='foo_set.all')
            fk_field = sfields.ListField(sfield=sfields.EmbeddedField(QuxSync), source='qux_set.all',
                                         reverse_rel='foo')
            emb_field = sfields.EmbeddedField(EggSync)
            dep_field = sfields.SyncField(mfield=mfields.IntField(), depends_on=Bar, source=lambda s, i: 10,
                                          reverse_rel='foo_set.all')
            dep_field2 = sfields.SyncField(mfield=mfields.StringField(), depends_on=Bar, source=lambda s, i: 'bar',
                                           reverse_rel='foo_set.all')

            class Meta:
                model = Foo
                collection = 'foos'
                id_field = 'id'
                fields = ('id', 'int_field', 'm2m_field', 'fk_field', 'emb_field', 'dep_field', 'dep_field2')

            @queryset_manager
            def cools(qs):
                return qs

        self.sync_cls = FooSync
        self.foo_sync = FooSync
        self.bar_sync = BarSync
        self.qux_sync = QuxSync
        self.egg_sync = EggSync
        self.model = Foo
        self.bar = Bar
        self.foo = Foo
        self.qux = Qux
        self.egg = Egg
