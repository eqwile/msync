from .factories import DocumentFactory


class QSBase(object):
    delim = '__'

    def __init__(self, sync_cls=None, document=None, sfield=None):
        self._sync_cls = sync_cls
        self._document = document
        self._sfield = sfield
        self._path = None

    def get_path(self):
        raise NotImplementedError()


class QSUpdate(QSBase):
    def get_path(self):
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
    def __init__(self, instance, **kwargs):
        self._instance = instance
        super(QSUpdateDependentField, self).__init__(**kwargs)

    def get_path(self):
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
    def get_path(self):
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

    def get_path(self):
        op = self._sfield.remove_operation()
        return {op + self.delim + k: v for k, v in self._pk.items()}
