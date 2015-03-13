import operator
from collections import defaultdict
from .factories import DocumentFactory


class QSBase(object):
    delim = '__'

    def __init__(self, sync_cls=None, document=None, instance=None, sfield=None, path=None):
        self._sync_cls = sync_cls
        self._document = document
        self._instance = instance
        self._sfield = sfield
        self._path = path

    def get_path(self):
        if self._path is None:
            self._path = self._get_path()
        return self._path

    def _get_path(self):
        return {}

    def __or__(self, other):
        return self.union(other)

    def union(self, other):
        path = self.get_path()
        path.update(other.get_path())
        return QSBase(sync_cls=self._sync_cls, document=self._document, sfield=self._sfield, path=path)


class QSPk(QSBase):
    def __init__(self, pk=None, **kwargs):
        self._pk = pk
        super(QSPk, self).__init__(**kwargs)

    def _get_path(self):
        sfield = self._sfield
        if sfield is None:
            sfield = self._sync_cls._meta.pk_sfield

        sfield_name = self._get_find_sfield_name(sfield)
        pk_value = self._get_instance_pk_value()

        if pk_value is not None and sfield.is_nested():
            sfield_name = '{}{}{}'.format(sfield_name, self.delim,
                                          sfield.get_nested_sync_cls()._meta.pk_sfield.name)

        return {sfield_name: pk_value}

    def _get_instance_pk_value(self):
        if self._pk is not None:
            return self._pk
        elif self._instance is not None:
            pk_field = self._instance._meta.pk
            return getattr(self._instance, pk_field.name)
        else:
            return None

    def _get_find_sfield_name(self, sfield):
        return self.delim.join([sf.name for sf in self._sync_cls._meta.get_sync_tree().get_sfield_path(sfield)])


class QSUpdate(QSBase):
    def _get_path(self):
        sfield_path = self._get_sfield_path()
        return self._build_path_of_simple_sfields(sfield_path)

    def _build_path_of_simple_sfields(self, sfield_path, op='set'):
        return {op + self.delim + sfield_path + self.delim + sf.name: getattr(self._document, sf.name)
                for sf in self._sfield.get_nested_sync_cls()._meta.get_simple_sfields()}

    def _get_sfield_path(self):
        parts = [sf.update_query_path()
                 for sf in self._sync_cls._meta.get_sync_tree().get_sfield_path(self._sfield)]
        return self.delim.join(parts)


class QSUpdateParent(QSUpdate):
    def _build_path_of_simple_sfields(self, sfield_path, op='set'):
        return {op + self.delim + sf.name: getattr(self._document, sf.name)
                for sf in self._sync_cls._meta.get_simple_sfields()}

    def _get_sfield_path(self):
        return None


class QSUpdateDependentField(QSBase):
    def _get_path(self):
        field_values = self._get_field_values()
        sfield_path = self._get_sfield_path()
        op = self._get_op()
        return {op + self.delim + sfield_path: field_values[self._sfield.name]}

    def _get_sfield_path(self):
        parts = [sf.name for sf in self._sync_cls._meta.get_sync_tree().get_sfield_path(self._sfield)]
        return self.delim.join(parts)

    def _get_field_values(self):
        return DocumentFactory.get_field_values_from_sources(self._instance, [self._sfield])

    def _get_op(self):
        return self._sfield.update_operation(new=True)


class QSClear(QSUpdateDependentField):
    def _get_field_values(self):
        return {self._sfield.name: []}

    def _get_op(self):
        return self._sfield.update_operation(new=False)


class QSCreate(QSBase):
    def _get_path(self):
        sfield_path = self._get_sfield_path()
        op = self._sfield.update_operation(new=True)
        return {op + self.delim + sfield_path: self._document}

    def _get_sfield_path(self):
        parts = [sf.name for sf in self._sync_cls._meta.get_sync_tree().get_sfield_path(self._sfield)]
        return self.delim.join(parts)


class QSDelete(QSBase):
    def __init__(self, pk=None, **kwargs):
        self._pk = pk
        super(QSDelete, self).__init__(**kwargs)

    def _get_path(self):
        op = self._sfield.remove_operation()
        if op == 'pull':
            pk = QSPk(sync_cls=self._sync_cls, pk=self._pk, instance=self._instance,
                      sfield=self._sfield).get_path()
        else:
            pk = QSPk(sync_cls=self._sync_cls, sfield=self._sfield).get_path()
        return {op + self.delim + k: v for k, v in pk.items()}


class BatchQuery(object):
    def __init__(self, sync_cls):
        self._sync_cls = sync_cls
        self._qs_collection = defaultdict(list)

    @property
    def qs_collection(self):
        return self._qs_collection

    def __enter__(self):
        self._qs_collection.clear()

    def __exit__(self, t, value, traceback):
        self.run()

    def run(self):
        for pk_path, qss in self._qs_collection.items():
            qs = reduce(operator.or_, qss)
            self._sync_cls._meta.document.objects.filter(**dict(pk_path)).update(**qs.get_path())

    def __setitem__(self, key, qs):
        try:
            ins, sfield = key
        except TypeError:
            ins, sfield = key, None

        pk_path = QSPk(sync_cls=self._sync_cls, instance=ins, sfield=sfield).get_path()
        self._qs_collection[frozenset(pk_path.items())].append(qs)
