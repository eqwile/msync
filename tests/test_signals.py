import pytest
from django.db import models
from msync.syncers import DocumentSync, EmbeddedSync


class TestSignals:
    def setup(self):
        class Foo(models.Model):
            field1 = models.IntegerField()
            field2 = models.IntegerField()
            field3 = models.IntegerField()

        class FooSync(DocumentSync):
            class Meta:
                fields = ('field1', 'field2')
                model = Foo
        self.sync_cls = FooSync

    
                
