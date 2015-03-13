# -*- coding: utf-8 -*-
from django.db.models import signals
from .queryset import (BatchQuery, QSUpdate, QSUpdateDependentField, QSUpdateParent, QSPk, QSClear,
                       QSDelete, QSCreate)


class SignalConnector(object):
    def __init__(self, sync_cls):
        self.parent_sync_cls = sync_cls
        self.parent_meta = sync_cls._meta
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
        signal.connect(handler, sender=model, weak=False, dispatch_uid=dispatch_uid)

    def _post_save_handler(self, instance, raw, created, using, update_fields, **kwargs):
        if self.is_m2m_through_model_of_parent(instance.__class__) and created:
            return

        with BatchQuery(self.parent_sync_cls) as b:
            nested_sfields = self.nested_model_sfields_dict[instance.__class__]
            for sfield in nested_sfields:
                sync_cls = sfield.get_nested_sync_cls()
                document = sync_cls.create_document(instance, with_embedded=created)

                qs = QSUpdate(sync_cls=self.parent_sync_cls, document=document, sfield=sfield)
                if created:
                    par_ins = sfield.get_reverse_rel()(instance)
                    for pi in par_ins:
                        b[pi] = qs
                else:
                    b[(instance, sfield)] = qs

            dependent_sfields = self.depends_on_model_sfields_dict[instance.__class__]
            for sfield in dependent_sfields:
                if sfield.is_belongs_to_parent(instance):
                    parent_instances = sfield.get_reverse_rel()(instance)
                    for pi in parent_instances:
                        b[pi] = QSUpdateDependentField(sync_cls=self.parent_sync_cls, instance=pi,
                                                       sfield=sfield)

            if not nested_sfields and not dependent_sfields and self.parent_meta.pass_filter(instance):
                document = self.parent_sync_cls.create_document(instance, with_embedded=created)
                if created:
                    print '{}.save()'.format(self.parent_sync_cls)
                    document.save()
                else:
                    b[instance] = QSUpdateParent(sync_cls=self.parent_sync_cls, document=document)

    def _post_delete_handler(self, instance, using, **kwargs):
        with BatchQuery(self.parent_sync_cls) as b:
            nested_sfields = self.nested_model_sfields_dict[instance.__class__]
            for sfield in nested_sfields:
                b[(instance, sfield)] = QSDelete(sync_cls=self.parent_sync_cls, sfield=sfield,
                                                 instance=instance)

            dependent_sfields = self.depends_on_model_sfields_dict[instance.__class__]
            for sfield in dependent_sfields:
                if sfield.is_belongs_to_parent(instance):
                    parent_instances = sfield.get_reverse_rel()(instance)
                    for pi in parent_instances:
                        b[pi] = QSUpdateDependentField(sync_cls=self.parent_sync_cls, instance=pi,
                                                       sfield=sfield)

            if not nested_sfields and not dependent_sfields and self.parent_meta.pass_filter(instance):
                pk_path = QSPk(sync_cls=self.parent_sync_cls, instance=instance).get_path()
                print '{}.filter({}).delete()'.format(self.parent_sync_cls, pk_path)
                self.parent_meta.document.objects.filter(**pk_path).delete()

    def _m2m_changed_handler(self, action, instance, model, pk_set, **kwargs):
        with BatchQuery(self.parent_sync_cls) as b:
            if action == 'post_add':
                model_instances = model.objects.filter(pk__in=pk_set)
                for model_instance in model_instances:
                    nested_sfields = self.nested_model_sfields_dict[model_instance.__class__]
                    for sfield in nested_sfields:
                        sync_cls = sfield.get_nested_sync_cls()
                        document = sync_cls.create_document(model_instance, with_embedded=True)
                        b[instance] = QSCreate(sync_cls=self.parent_sync_cls, document=document,
                                               sfield=sfield)

            elif action == 'post_remove':
                for pk in pk_set:
                    nested_sfields = self.nested_model_sfields_dict[model]
                    for sfield in nested_sfields:
                        b[instance] = QSDelete(sync_cls=self.parent_sync_cls, sfield=sfield, pk=pk)

            elif action == 'post_clear':
                nested_sfields = self.nested_model_sfields_dict[model]
                for sfield in nested_sfields:
                    b[(instance, sfield)] = QSClear(sync_cls=self.parent_sync_cls, sfield=sfield,
                                                    instance=instance)

            elif action not in ('pre_add', 'pre_remove', 'pre_clear'):
                raise TypeError('Get %s, %s, %s' % (action, instance, model))
