# -*- coding: utf-8 -*-
from functools import partial
from django.db.models import signals
from .queryset import (BatchQuery, QSUpdate, QSUpdateDependentField, QSUpdateParent, QSPk, QSClear,
                       QSDelete, QSCreate, BatchTask)


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

    def _is_nested_sfield_async(self, sfield):
        if sfield.async is not None:
            return sfield.async
        return sfield.get_nested_sync_cls()._meta.async

    def _is_dependent_sfield_async(self, sfield):
        return sfield.async

    def _is_parent_sync_async(self):
        return self.parent_sync_cls._meta.async

    def _post_save_handler(self, instance, raw, created, using, update_fields, **kwargs):
        if self.is_m2m_through_model_of_parent(instance.__class__) and created:
            return

        with BatchQuery(self.parent_sync_cls) as b, BatchTask(self.parent_sync_cls) as t:
            nested_sfields = self.nested_model_sfields_dict[instance.__class__]
            for sfield in nested_sfields:
                if update_fields and not sfield.get_nested_sync_cls().has_some_field(update_fields):
                    continue

                task = partial(save_nested_sfield, parent_sync_cls=self.parent_sync_cls, sfield=sfield,
                               instance=instance, created=created)

                if self._is_nested_sfield_async(sfield):
                    t.add(task)
                else:
                    task(b)

            dependent_sfields = self.depends_on_model_sfields_dict[instance.__class__]
            for sfield in dependent_sfields:
                if sfield.is_belongs_to_parent(instance):

                    task = partial(save_dependent_sfield, parent_sync_cls=self.parent_sync_cls,
                                   sfield=sfield, instance=instance)

                    if self._is_dependent_sfield_async(sfield):
                        t.add(task)
                    else:
                        task(b)

            if not nested_sfields and not dependent_sfields and self.parent_meta.pass_filter(instance):
                if update_fields and not self.parent_sync_cls.has_some_field(update_fields):
                    return

                task = partial(save_parent_sfields, parent_sync_cls=self.parent_sync_cls, instance=instance,
                               created=created)

                if self._is_parent_sync_async():
                    t.add(task)
                else:
                    task(b)

    def _post_delete_handler(self, instance, using, **kwargs):
        with BatchQuery(self.parent_sync_cls) as b, BatchTask(self.parent_sync_cls) as t:
            nested_sfields = self.nested_model_sfields_dict[instance.__class__]
            for sfield in nested_sfields:

                task = partial(delete_nested_sfield, parent_sync_cls=self.parent_sync_cls, sfield=sfield,
                               instance=instance)

                if self._is_nested_sfield_async(sfield):
                    t.add(task)
                else:
                    task(b)

            dependent_sfields = self.depends_on_model_sfields_dict[instance.__class__]
            for sfield in dependent_sfields:
                if sfield.is_belongs_to_parent(instance):

                    task = partial(delete_dependent_sfield, parent_sync_cls=self.parent_sync_cls,
                                   instance=instance, sfield=sfield)

                    if self._is_dependent_sfield_async(sfield):
                        t.add(task)
                    else:
                        task(b)

            if not nested_sfields and not dependent_sfields and self.parent_meta.pass_filter(instance):
                task = partial(delete_parent, parent_sync_cls=self.parent_sync_cls, instance=instance,
                               parent_meta=self.parent_meta)

                if self._is_parent_sync_async():
                    t.add(task)
                else:
                    task(b)

    def _m2m_changed_handler(self, action, instance, model, pk_set, **kwargs):
        if action not in ('post_add', 'post_remove', 'post_clear'):
            return

        with BatchQuery(self.parent_sync_cls) as b, BatchTask(self.parent_sync_cls) as t:
            nested_sfields = self.nested_model_sfields_dict[model]
            for sfield in nested_sfields:
                task = None

                if action == 'post_add':
                    task = partial(m2m_post_add, parent_sync_cls=self.parent_sync_cls, sfield=sfield,
                                   pk_set=pk_set, model=model, instance=instance)
                elif action == 'post_remove':
                    task = partial(m2m_post_remove, parent_sync_cls=self.parent_sync_cls, sfield=sfield,
                                   pk_set=pk_set, instance=instance)
                elif action == 'post_clear':
                    task = partial(m2m_post_clear, parent_sync_cls=self.parent_sync_cls, sfield=sfield,
                                   instance=instance)

                if task is not None:
                    if self._is_nested_sfield_async(sfield):
                        t.add(task)
                    else:
                        task(b)


def save_nested_sfield(batch, parent_sync_cls=None, sfield=None, instance=None, created=None):
    sync_cls = sfield.get_nested_sync_cls()
    document = sync_cls.create_document(instance, with_embedded=created)

    if created:
        par_ins = sfield.get_reverse_rel()(instance)
        for pi in par_ins:
            batch[pi] = QSCreate(sync_cls=parent_sync_cls, document=document, sfield=sfield)
    else:
        batch[(instance, sfield)] = QSUpdate(sync_cls=parent_sync_cls, document=document,
                                             sfield=sfield)


def save_dependent_sfield(batch, parent_sync_cls=None, sfield=None, instance=None):
    parent_instances = sfield.get_reverse_rel()(instance)
    for pi in parent_instances:
        batch[pi] = QSUpdateDependentField(sync_cls=parent_sync_cls, instance=pi,
                                           sfield=sfield)


def save_parent_sfields(batch, parent_sync_cls=None, instance=None, created=None):
    document = parent_sync_cls.create_document(instance, with_embedded=created)
    if created:
        print('{}.save()'.format(parent_sync_cls))
        document.save()
    else:
        batch[instance] = QSUpdateParent(sync_cls=parent_sync_cls, document=document)


def delete_nested_sfield(batch, parent_sync_cls=None, sfield=None, instance=None):
    batch[(instance, sfield)] = QSDelete(sync_cls=parent_sync_cls, sfield=sfield, instance=instance)


def delete_dependent_sfield(batch, parent_sync_cls=None, instance=None, sfield=None):
    parent_instances = sfield.get_reverse_rel()(instance)
    for pi in parent_instances:
        batch[pi] = QSUpdateDependentField(sync_cls=parent_sync_cls, instance=pi, sfield=sfield)


def delete_parent(_, parent_sync_cls=None, parent_meta=None, instance=None):
    pk_path = QSPk(sync_cls=parent_sync_cls, instance=instance).get_path()
    print('{}.filter({}).delete()'.format(parent_sync_cls, pk_path))
    parent_meta.document.objects.filter(**pk_path).delete()


def m2m_post_add(_, parent_sync_cls=None, sfield=None, pk_set=None, model=None, instance=None):
    sync_cls = sfield.get_nested_sync_cls()
    model_instances = model.objects.filter(pk__in=pk_set)

    documents = [sync_cls.create_document(model_instance, with_embedded=True)
                 for model_instance in model_instances]

    if not documents:
        return

    with BatchQuery(parent_sync_cls) as b:
        b[instance] = QSCreate(sync_cls=parent_sync_cls, documents=documents, sfield=sfield)


def m2m_post_remove(_, parent_sync_cls=None, sfield=None, pk_set=None, instance=None):
    for pk in pk_set:
        with BatchQuery(parent_sync_cls) as b:
            b[instance] = QSDelete(sync_cls=parent_sync_cls, sfield=sfield, pk=pk)


def m2m_post_clear(batch, parent_sync_cls=None, sfield=None, instance=None):
    batch[instance] = QSClear(sync_cls=parent_sync_cls, sfield=sfield, instance=instance)


def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


import copy_reg
import types
copy_reg.pickle(types.MethodType, _pickle_method)
