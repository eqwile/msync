# -*- coding: utf-8 -*-
import functools
from django.db.models import signals
from .options import SyncTree
from .factories import DocumentFactory


class SignalConnector(object):
    def __init__(self, sync_cls):
        self.parent_sync_cls = sync_cls
        self.parent_meta = sync_cls._meta
        self.document_search = DocumentSearch(sync_cls)
        # maybe here we can use only own sfields?
        self.nested_model_sfields_dict = self.parent_meta.get_nested_model_sfields_dict()
        self.depends_on_model_sfields_dict = self.parent_meta.get_depends_on_model_sfields_dict()

    def is_m2m_through_model_of_parent(self, model):
        rel_objects = model._meta.get_all_related_many_to_many_objects()
        return any(rel.model == self.parent_meta.model for rel in rel_objects)

    def get_m2m_through_model_if_exists(self, model):
        rel_objects = model._meta.get_all_related_many_to_many_objects()
        return next((rel.field.rel.through for rel in rel_objects if rel.model == self.parent_meta.model), None)

    def setup(self):
        for model in self.parent_meta.get_all_own_models():
            self.connect_signal(signals.post_save, self._post_save_handler, model)
            self.connect_signal(signals.post_delete, self._post_delete_handler, model)

            through_model = self.get_m2m_through_model_if_exists(model)
            if through_model is not None:
                self.connect_signal(signals.m2m_changed, self._m2m_changed_handler, through_model)

        if self.parent_meta.model is not None:
            self.connect_signal(signals.post_save, self._post_save_handler, self.parent_sync_cls._meta.model)
            self.connect_signal(signals.post_delete, self._post_delete_handler, self.parent_sync_cls._meta.model)

    def connect_signal(self, signal, handler, model):
        dispatch_uid = '{}-{}-{}'.format(self.parent_sync_cls.__name__, model.__name__, handler.__name__)
        # print dispatch_uid
        signal.connect(handler, sender=model, weak=False, dispatch_uid=dispatch_uid)

    def _post_save_handler(self, instance, raw, created, using, update_fields, **kwargs):
        if self.is_m2m_through_model_of_parent(instance.__class__) and created:
            return

        print
        print self.parent_sync_cls
        ############################################################################################
        # Update nested documents

        # TODO: maybe merge updating
        nested_sfields = self.nested_model_sfields_dict[instance.__class__]
        for sfield in nested_sfields:
            sync_cls = sfield.get_nested_sync_cls()
            document = sync_cls.create_document(instance, with_embedded=created)
            self.document_search.update_documents(sfield, instance, document, new=created)

        ############################################################################################
        # Update dependent fields
        dependent_sfields = self.depends_on_model_sfields_dict[instance.__class__]
        for sfield in dependent_sfields:
            if sfield.is_belongs_to_parent(instance):
                self.document_search.update_mfield(sfield, instance)

        ############################################################################################
        # Update parent itself if necessary
        if not nested_sfields and not dependent_sfields and self.parent_meta.pass_filter(instance):
            document = self.parent_sync_cls.create_document(instance, with_embedded=created)
            self.document_search.update_documents(None, instance, document, new=created)

    def _post_delete_handler(self, instance, using, **kwargs):
        print
        print 'REMOVE'
        print self.parent_sync_cls

        ############################################################################################
        # remove nested documents
        nested_sfields = self.nested_model_sfields_dict[instance.__class__]
        for sfield in nested_sfields:
            self.document_search.remove_documents(sfield, instance)

        ############################################################################################
        # update dependent fields
        dependent_sfields = self.depends_on_model_sfields_dict[instance.__class__]
        for sfield in dependent_sfields:
            if sfield.is_belongs_to_parent(instance):
                self.document_search.update_mfield(sfield, instance)

        ############################################################################################
        # remove parent itself
        if not nested_sfields and not dependent_sfields and self.parent_meta.pass_filter(instance):
            self.document_search.remove_documents(None, instance)

    def _m2m_changed_handler(self, action, instance, model, pk_set, **kwargs):
        print
        print 'm2m_changed'
        print kwargs, action
        if action == 'post_add':
            model_instances = model.objects.filter(pk__in=pk_set)
            for model_instance in model_instances:
                nested_sfields = self.nested_model_sfields_dict[model_instance.__class__]
                for sfield in nested_sfields:
                    sync_cls = sfield.get_nested_sync_cls()
                    document = sync_cls.create_document(model_instance, with_embedded=True)
                    self.document_search.update_documents(sfield, model_instance, document, new=True,
                                                          parent_instances=[instance])
        elif action == 'post_remove':
            for pk in pk_set:
                nested_sfields = self.nested_model_sfields_dict[model]
                for sfield in nested_sfields:
                    self.document_search.remove_documents(sfield, None, pk=pk)
        elif action == 'post_clear':
            nested_sfields = self.nested_model_sfields_dict[model]
            for sfield in nested_sfields:
                self.document_search.update_mfield(sfield, None, value=[], parent_instances=[instance])
        elif action not in ('pre_add', 'pre_remove', 'pre_clear'):
            raise TypeError('Get %s, %s, %s' % (action, instance, model))


class DocumentSearch(object):
    def __init__(self, sync_cls):
        self.parent_sync_cls = sync_cls
        self.parent_document = sync_cls._meta.document
        self.qb = QueryBuilder(sync_cls)

    def find_documents(self, sfield, instance, pk=None):
        query = self.qb.build_pk_path(sfield, instance, pk=pk)
        print 'query:', query
        return self.parent_document.objects.filter(**query)

    def find_document(self, sfield, instance, pk=None):
        query = self.qb.build_pk_path(sfield, instance, pk=pk)
        print 'query:', query
        return self.parent_document.objects.get(**query)

    def update_documents(self, sfield, instance, document, new=False, parent_instances=None):
        if not new:
            update_path = self.qb.build_update_path(sfield, document)
            print 'update_path:', update_path
            self.find_documents(sfield, instance).update(**update_path)
        elif sfield is not None:
            update_path = self.qb.build_update_path_of_nested_sfield(sfield, document)
            print 'update_path:', update_path
            #TODO: optimize query
            par_ins = sfield.get_reverse_rel()(instance) if parent_instances is None else parent_instances
            for pi in par_ins:
                self.find_documents(None, pi).update(**update_path)
        elif new:
            print 'create'
            document.save()

    def remove_documents(self, sfield, instance, pk=None):
        documents = self.find_documents(sfield, instance, pk=pk)
        if sfield is None:
            documents.delete()
        else:
            remove_path = self.qb.build_remove_path(sfield, instance, pk=pk)
            print 'remove_path:', remove_path
            documents.update(**remove_path)

    def update_mfield(self, sfield, instance, value=None, parent_instances=None):
        print sfield
        parent_instances = sfield.get_reverse_rel()(instance) if parent_instances is None else parent_instances
        for pi in parent_instances:
            field_value = {sfield.name: value} if value is not None else None
            update_kwargs = self.qb.build_update_depends_on_sfield(sfield, pi, field_values=field_value)
            print 'udpate_kwarg:', update_kwargs
            self.find_documents(None, pi).update(**update_kwargs)


class QueryBuilder(object):
    def __init__(self, sync_cls):
        self.parent_sync_cls = sync_cls

    def build_pk_path(self, sfield, instance, pk=None):
        if sfield is None:
            sfield = self.parent_sync_cls._meta.pk_sfield

        sfield_name = self._get_find_sfield_name(sfield)
        if sfield.is_nested():
            sfield_name = '{}__{}'.format(sfield_name, sfield.get_nested_sync_cls()._meta.pk_sfield.name)

        pk_value = self._get_instance_pk_value(instance, pk=pk)
        return {sfield_name: pk_value}

    def build_update_path(self, sfield, document, new=False):
        sfield_path = None if sfield is None else self._get_update_sfield_name(sfield, new=new)
        return self._build_path_of_simple_sfields(document, sfield, sfield_path)

    def build_update_path_of_nested_sfield(self, sfield, document):
        sfield_path = self._get_update_sfield_name(sfield, new=True)
        op = sfield.update_operation(True)
        return {op + '__' + sfield_path: document}

    def build_update_depends_on_sfield(self, sfield, instance, field_values=None):
        new = field_values is None
        if field_values is None:
            field_values = DocumentFactory.get_field_values_from_sources(instance, [sfield])
        sfield_name = self._get_update_sfield_name(sfield, new=True)
        op = sfield.update_operation(new)
        return {op + '__' + sfield_name: field_values[sfield.name]}

    def build_remove_path(self, sfield, instance, pk=None):
        sfield_kwarg = self.build_pk_path(sfield, instance, pk=pk)
        op = sfield.remove_operation()
        return {'{}__{}'.format(op, k): v for k, v in sfield_kwarg.items()}

    def _build_path_of_simple_sfields(self, document, sfield, sfield_path, op='set'):
        if sfield is None:
            return {op + '__' + sf.name: getattr(document, sf.name)
                    for sf in self.parent_sync_cls._meta.get_simple_sfields()}
        else:
            return {op + '__' + sfield_path + '__' + sf.name: getattr(document, sf.name)
                    for sf in sfield.get_nested_sync_cls()._meta.get_simple_sfields()}

    def _get_update_sfield_name(self, sfield, new):
        get_name = lambda sf: sf.name if new else sf.update_query_path()
        return '__'.join([get_name(sf) for sf in self.parent_sync_cls._meta.get_sync_tree().get_sfield_path(sfield)])

    def _get_find_sfield_name(self, sfield):
        return '__'.join([sf.name for sf in self.parent_sync_cls._meta.get_sync_tree().get_sfield_path(sfield)])

    def _get_instance_pk_value(self, instance, pk=None):
        if pk is not None:
            return pk
        pk_field = instance._meta.pk
        return getattr(instance, pk_field.name)
