# -*- coding: utf-8 -*-
from collections import defaultdict
from django.db.models.fields import FieldDoesNotExist
from .factories import SyncFieldFactory
from .utils import Tree


class Options(object):
    _collection_setting_keys = ('allow_inheritance', 'collection', 'id_field', 'max_documents',
                                'max_size', 'indexes', 'index_options', 'index_background',
                                'index_drop_dups', 'index_cls', 'ordering', 'shard_key', 'abstract',
                                'queryset_class', 'auto_create_index')

    def __init__(self, sync_cls, meta, sync_bases, document_type=None):
        self._sync_cls = sync_cls

        self.model = getattr(meta, 'model', None)
        self.filter = getattr(meta, 'filter', None)
        self.exclude = getattr(meta, 'exclude', tuple())
        self.fields = getattr(meta, 'fields', None)
        self.async = getattr(meta, 'async', False)
        self._field_names = self._get_field_names_from_meta_fields()

        self.document_type = document_type
        self.document = None

        # TODO: later implement caching by decorator
        self._sfields = []
        self._sfields_dict = {}
        self.__sfields_cache = None
        self.__sfields_dict_cache = None

        self._pk_sfield = None
        self.__pk_sfield_cache = None

        self._qs_managers = {}

        self.sync_tree = None
        self.own_sync_tree = None
        self.collection_settings = self.get_collection_settings(meta)
        self.bases = self._get_sync_bases(sync_bases)

    def add_field(self, name, field):
        if name in self.exclude or (self._field_names is not None and name not in self._field_names):
            return

        self._sfields.append(field)
        self._sfields_dict[name] = field
        self.sync_tree = None
        self.own_sync_tree = None
        self.__sfields_dict_cache = None
        self.__sfields_cache = None

    def add_qs_manager(self, name, qsm):
        self._qs_managers[name] = qsm

    def setup_pk(self, sfield):
        self._pk_sfield = sfield
        self.__pk_sfield_cache = None

    def get_collection_settings(self, meta_cls):
        return {key: getattr(meta_cls, key) for key in self._collection_setting_keys if hasattr(meta_cls, key)}

    def _add_model_fields(self):
        if self.model is None:
            return []

        model_meta = self.model._meta
        field_names = self.fields if self.fields is not None else model_meta.get_all_field_names()
        field_name_source = self._normalize_meta_fields(field_names)
        already_created_sfield_names = self.sfields_dict.keys()

        sfield_factory = SyncFieldFactory(self._sync_cls)
        for name, source in field_name_source:
            if name not in self.exclude and name not in already_created_sfield_names:
                try:
                    field, _, _, _ = model_meta.get_field_by_name(source)
                except FieldDoesNotExist:
                    raise TypeError('Cannot find field: %s' % source)
                else:
                    sfield = sfield_factory.create(field, source=source, name=name)
                    self._sync_cls.add_to_class(name, sfield)

    def _normalize_meta_fields(self, fields):
        norm_fields = []
        for field in fields:
            if isinstance(field, basestring):
                norm_fields.append((field, field))
            elif isinstance(field, (tuple, list)) and len(field) == 2:
                norm_fields.append(field)
            else:
                raise TypeError('Come on! Something is wrong with the field: %s' % field)
        return norm_fields

    def _get_field_names_from_meta_fields(self):
        if self.fields is None:
            return None
        return [name for name, _ in self._normalize_meta_fields(self.fields)]

    def _set_my_ass(self, value):
        raise TypeError('Immutable, bitch!')

    def _get_sfields(self):
        if self.__sfields_cache is None:
            self.__sfields_cache = self.sfields_dict.values()
        return self.__sfields_cache

    sfields = property(_get_sfields, _set_my_ass)

    def _get_own_sfields(self):
        return self._sfields

    own_sfields = property(_get_own_sfields, _set_my_ass)

    def _get_sfields_dict(self):
        if self.__sfields_dict_cache is None:
            self.__sfields_dict_cache = {}
            for base in reversed(self.bases):
                self.__sfields_dict_cache.update(base.sfields_dict)
            self.__sfields_dict_cache.update(self._sfields_dict)

            for name in self.exclude:
                del self.__sfields_dict_cache[name]
        return self.__sfields_dict_cache

    sfields_dict = property(_get_sfields_dict, _set_my_ass)

    def _get_own_sfields_dict(self):
        return self._sfields_dict

    own_sfields_dict = property(_get_own_sfields_dict, _set_my_ass)

    def _get_pk_sfield(self):
        if self.__pk_sfield_cache is None:
            self.__pk_sfield_cache = self._pk_sfield
            if self.__pk_sfield_cache is None:
                for base in self.bases:
                    if base.pk_sfield is not None:
                        self.__pk_sfield_cache = base.pk_sfield
                        return self.__pk_sfield_cache
        return self.__pk_sfield_cache

    pk_sfield = property(_get_pk_sfield, _set_my_ass)

    def is_need_to_connect_signals(self):
        return self.model is not None or self.collection_settings.get('allow_inheritance', False)

    def get_document_bases(self):
        if not self.bases:
            return (self.document_type,)
        return tuple(base.document for base in self.bases)

    def pass_filter(self, instance):
        if self.filter is not None:
            return self.filter(instance)
        return True

    def get_nested_sfields(self):
        return self._filter_sfields(self.get_sync_tree(), lambda sf: sf.is_nested())

    def get_depends_on_sfields(self):
        return self._filter_sfields(self.get_sync_tree(), lambda sf: sf.is_depens_on())

    def get_own_nested_sfields(self):
        return self._filter_sfields(self.get_own_sync_tree(), lambda sf: sf.is_nested())

    def get_own_depends_on_sfields(self):
        return self._filter_sfields(self.get_own_sync_tree(), lambda sf: sf.is_depens_on())

    def _filter_sfields(self, sync_tree, flt):
        all_sfields = sync_tree.get_all_sfields()
        return list({sf for sf in all_sfields if flt(sf)})

    def get_all_models(self):
        return list(self.get_nested_models() | self.get_depends_on_models())

    def get_all_own_models(self):
        return list(self.get_own_nested_models() | self.get_own_depends_on_models())

    def get_nested_models(self):
        return {sf.get_nested_sync_cls()._meta.model for sf in self.get_nested_sfields()}

    def get_depends_on_models(self):
        return {sf.get_depends_on_model() for sf in self.get_depends_on_sfields()}

    def get_own_nested_models(self):
        return {sf.get_nested_sync_cls()._meta.model for sf in self.get_own_nested_sfields()}

    def get_own_depends_on_models(self):
        return {sf.get_depends_on_model() for sf in self.get_own_depends_on_sfields()}

    def get_nested_model_sfields_dict(self):
        # TODO: cache
        d = defaultdict(list)
        for sf in self.get_nested_sfields():
            d[sf.get_nested_sync_cls()._meta.model].append(sf)
        return d

    def get_depends_on_model_sfields_dict(self):
        # TODO: cache
        d = defaultdict(list)
        for sf in self.get_depends_on_sfields():
            d[sf.get_depends_on_model()].append(sf)
        return d

    def get_simple_sfields(self):
        return [sfield for sfield in self.sfields if not sfield.is_nested() and not sfield.is_depens_on()]

    def get_sync_tree(self):
        if self.sync_tree is None:
            self.sync_tree = SyncTree(sfields=self.sfields)
        return self.sync_tree

    def get_own_sync_tree(self):
        if self.own_sync_tree is None:
            self.own_sync_tree = SyncTree(sfields=self.own_sfields)
        return self.own_sync_tree

    def _get_sync_bases(self, sync_bases):
        return [getattr(base, '_meta') for base in sync_bases if hasattr(base, '_meta')]


class SyncTree(object):
    def __init__(self, sync_cls=None, sfields=None):
        if sync_cls is not None:
            sfields = sync_cls._meta.sfields
        elif sfields is None:
            raise TypeError('SyncTree')

        self._tree = self._create_tree(sfields)

    def get_all_sfields(self):
        return self._get_all_sfields([], self._tree)

    def get_sfield_path(self, sfield):
        return self._get_sfield_path(self._tree, sfield)

    def pr(self):
        for k in self._tree:
            self._pr(k, self._tree[k], 0)

    def _pr(self, name, tree, offset):
        print '%s%s(%s)' % (' ' * offset, name, hash(name))
        for k in tree:
            self._pr(k, tree[k], offset + 4)

    def _get_all_sfields(self, all_sfields, tree):
        for sf in tree:
            all_sfields.append(sf)
            self._get_all_sfields(all_sfields, tree[sf])
        return all_sfields

    def _get_sfield_path(self, tree, sfield):
        if sfield in tree:
            return [sfield]

        for sf in tree:
            path = self._get_sfield_path(tree[sf], sfield)
            if path:
                path.insert(0, sf)
                return path
        return []
                
    def _create_tree(self, sfields):
        tree = Tree()
        for sfield in sfields:
            tree[sfield] = self._create_tree_of_sfield(sfield)
        return tree

    def _create_tree_of_sfield(self, sfield):
        if sfield.is_nested():
            nested_sync_cls = sfield.get_nested_sync_cls()
            return self._create_tree(nested_sync_cls._meta.sfields)
        return Tree()
