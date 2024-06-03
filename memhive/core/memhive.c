#include "errormech.h"
#include "memhive.h"
#include "module.h"
#include "queue.h"
#include "track.h"


static int
memhive_tp_init(MemHive *o, PyObject *args, PyObject *kwds)
{
    module_state *state = MemHive_GetModuleStateByPythonType(Py_TYPE(o));

    if (pthread_rwlock_init(&o->index_rwlock, NULL)) {
        Py_FatalError("Failed to initialize an RWLock");
    }

    o->index = MemHive_NewMap(state);
    if (o->index == NULL) {
        goto err;
    }

    o->mod_state = state;

    if (MemQueue_Init(&o->subs_health, 0)) {
        Py_FatalError("Failed to initialize the system queue");
    }
    if (MemQueue_Init(&o->for_subs, MEMHIVE_MAX_WORKERS)) {
        Py_FatalError("Failed to initialize the subs intake queue");
    }
    if (MemQueue_Init(&o->for_main, 0)) {
        Py_FatalError("Failed to initialize the subs output queue");
    }

    o->subs_list = NULL;
    if (pthread_mutex_init(&o->subs_list_mut, NULL)) {
        Py_FatalError("Failed to initialize a mutex");
    }

    o->push_id_cnt = 0;

    TRACK(state, o);

    return 0;

err:
    Py_CLEAR(o->index);
    return -1;
}

MEMHIVE_REMOTE(ssize_t)
MemHive_RegisterSub(MemHive *hive, MemHiveSub *sub,
                    module_state *remote_state)
{
    pthread_mutex_lock(&hive->subs_list_mut);

    // Adding channel might fail, so do it first.
    ssize_t channel = MemQueue_AddChannel(&hive->for_subs, remote_state);
    if (channel < 0) {
        goto err_from_locked;
    }

    SubsList *cnt = PyMem_RawMalloc(sizeof (SubsList));
    if (cnt == NULL) {
        PyErr_NoMemory();
        goto err_from_locked;
    }

    cnt->sub = sub;
    cnt->next = NULL;

    if (hive->subs_list == NULL) {
        hive->subs_list = cnt;
    } else {
        cnt->next = hive->subs_list;
        hive->subs_list = cnt;
    }

    pthread_mutex_unlock(&hive->subs_list_mut);
    return channel;

err_from_locked:
    pthread_mutex_unlock(&hive->subs_list_mut);
    return -1;
}

MEMHIVE_REMOTE(void)
MemHive_UnregisterSub(MemHive *hive, MemHiveSub *sub)
{
    int removed = 0;

    pthread_mutex_lock(&hive->subs_list_mut);

    SubsList *temp = hive->subs_list;
    SubsList *prev = NULL;

    if (temp != NULL && temp->sub == sub) {
        hive->subs_list = temp->next;
        PyMem_RawFree(temp);
        removed = 1;
    } else {
        while (temp != NULL && temp->sub != sub) {
            prev = temp;
            temp = temp->next;
        }
        if (temp != NULL) {
            prev->next = temp->next;
            PyMem_RawFree(temp);
            removed = 1;
        }
    }

    pthread_mutex_unlock(&hive->subs_list_mut);

    assert(removed);
}

static void
memhive_do_refs(MemHive *hive)
{
    pthread_mutex_lock(&hive->subs_list_mut);

    SubsList *lst = hive->subs_list;
    while (lst != NULL) {
        MemHive_RefQueue_Run(lst->sub->main_refs, hive->mod_state);
        lst = lst->next;
    }

    pthread_mutex_unlock(&hive->subs_list_mut);
}


static void
memhive_tp_dealloc(MemHive *o)
{
    if (pthread_rwlock_wrlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index write lock");
    }

    Py_CLEAR(o->index);

    if (pthread_rwlock_unlock(&o->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index write lock");
    }

    if (pthread_rwlock_destroy(&o->index_rwlock)) {
        Py_FatalError("Failed to destroy the MemHive index lock");
    }

    pthread_mutex_destroy(&o->subs_list_mut);
    SubsList *l = o->subs_list;
    while (l != NULL) {
        SubsList *next = l->next;
        PyMem_RawFree(l);
        l = next;
    }
    o->subs_list = NULL;

    MemQueue_Destroy(&o->subs_health);
    MemQueue_Destroy(&o->for_main);
    MemQueue_Destroy(&o->for_subs);

    PyTypeObject *tp = Py_TYPE(o);
    tp->tp_free((PyObject *)o);
    Py_DecRef((PyObject*)tp);
}


static Py_ssize_t
memhive_tp_len(MemHive *o)
{
    if (pthread_rwlock_rdlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    Py_ssize_t size = PyObject_Length(o->index);

    if (pthread_rwlock_unlock(&o->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    return size;
}


static PyObject *
memhive_tp_subscript(MemHive *o, PyObject *key)
{
    return MemHive_Get(o->mod_state, o, key);
}


static int
memhive_tp_ass_sub(MemHive *o, PyObject *key, PyObject *val)
{
    if (pthread_rwlock_wrlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index write lock");
    }

    // It's important to do the refs here! When a remote interpreter reads
    // a value, say another Map, that Map has to keep existing in the main
    // interpreter. All scheduled increfs while `index_rwlock` *read* lock
    // was held must be applied before we apply changes while having
    // the *write* lock.
    memhive_do_refs(o);

    PyObject *new_index = MemHive_MapSetItem(o->mod_state, o->index, key, val);
    if (new_index != NULL) {
        Py_SETREF(o->index, new_index);
    }

    if (pthread_rwlock_unlock(&o->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index write lock");
    }

    if (new_index == NULL) {
        return -1;
    }

    return 0;
}

static int
memhive_tp_contains(MemHive *hive, PyObject *key)
{
    return MemHive_MapContains(hive->mod_state, hive->index, key);
}


Py_ssize_t
MemHive_Len(MemHive *hive)
{
    return memhive_tp_len(hive);
}

MEMHIVE_REMOTE(PyObject *)
MemHive_Get(module_state *state, MemHive *hive, PyObject *key)
{
    if (pthread_rwlock_rdlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    PyObject *val = MemHive_MapGetItem(state, hive->index, key, NULL);

    if (pthread_rwlock_unlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    return val;
}

MEMHIVE_REMOTE(int)
MemHive_Contains(module_state *state, MemHive *hive, PyObject *key)
{
    if (pthread_rwlock_rdlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    int ret = MemHive_MapContains(state, hive->index, key);

    if (pthread_rwlock_unlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    return ret;
}

static PyObject *
memhive_py_push(MemHive *o, PyObject *val)
{
    TRACK(o->mod_state, val);
    if (MemQueue_HubPush(&o->for_subs, o->mod_state, 0, (PyObject*)o,
                         ++o->push_id_cnt, val))
    {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_broadcast(MemHive *o, PyObject *val)
{
    TRACK(o->mod_state, val);
    if (MemQueue_HubBroadcast(&o->for_subs, o->mod_state, (PyObject*)o, val)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_listen_subs_health(MemHive *o, PyObject *args)
{
    memqueue_event_t event;
    RemoteObject *sender;
    RemoteObject *remote_val;
    uint64_t id;

    if (MemQueue_Listen(&o->subs_health, o->mod_state, 0,
                        &event, &sender, &id, &remote_val))
    {
        return NULL;
    }

    switch (event) {
        case E_HEALTH_ERROR: {
            assert(remote_val != NULL);
            assert(PyTuple_CheckExact(remote_val));
            assert(Py_SIZE(remote_val) == 3);

            PyObject *err = MemHive_RestoreError(
                o->mod_state,
                (RemoteObject*)PyTuple_GET_ITEM(remote_val, 2)
            );
            if (MemHive_RefQueue_Dec(((MemHiveSub*)sender)->subs_refs,
                                     remote_val))
            {
                Py_XDECREF(err);
                return NULL;
            }

            if (err == NULL) {
                return NULL;
            }

            PyObject *err_name = MemHive_CopyObject(
                o->mod_state, (RemoteObject*)PyTuple_GET_ITEM(remote_val, 0));
            if (err_name == NULL) {
                Py_CLEAR(err);
                return NULL;
            }

            PyObject *err_msg = MemHive_CopyObject(
                o->mod_state, (RemoteObject*)PyTuple_GET_ITEM(remote_val, 1));
            if (err_msg == NULL) {
                Py_CLEAR(err_name);
                Py_CLEAR(err);
                return NULL;
            }

            PyObject *sub_id = PyLong_FromUnsignedLongLong(id);
            if (sub_id == NULL) {
                Py_CLEAR(err_name);
                Py_CLEAR(err_msg);
                Py_CLEAR(err);
                return NULL;
            }

            PyObject *ret = PyTuple_Pack(
                5, o->mod_state->str_ERROR, sub_id, err_name, err_msg, err
            );
            Py_DECREF(sub_id);
            Py_DECREF(err_name);
            Py_DECREF(err_msg);
            Py_DECREF(err);
            return ret;
        }

        case E_HEALTH_START: {
            PyObject *sub_id = PyLong_FromUnsignedLongLong(id);
            if (sub_id == NULL) {
                return NULL;
            }

            PyObject *ret = PyTuple_Pack(
                2, o->mod_state->str_START, sub_id
            );
            Py_DECREF(sub_id);
            return ret;
        }

        case E_HEALTH_CLOSE: {
            PyObject *sub_id = PyLong_FromUnsignedLongLong(id);
            if (sub_id == NULL) {
                return NULL;
            }

            PyObject *ret = PyTuple_Pack(
                2, o->mod_state->str_CLOSE, sub_id
            );
            Py_DECREF(sub_id);
            return ret;
        }

        default:
            Py_UNREACHABLE();
    }
}

static PyObject *
memhive_py_listen(MemHive *o, PyObject *args)
{
    memqueue_event_t event;
    RemoteObject *sender;
    RemoteObject *remote_val;
    uint64_t id;

    if (MemQueue_Listen(&o->for_main, o->mod_state, 0,
                        &event, &sender, &id, &remote_val))
    {
        return NULL;
    }


    PyObject *ret = NULL;

    PyObject *payload = MemHive_CopyObject(o->mod_state, remote_val);
    if (payload == NULL) {
        goto err;
    }

    MemHiveSub *sub = (MemHiveSub*)sender;

    if (MemHive_RefQueue_Dec(sub->subs_refs, remote_val)) {
        goto err;
    }

    switch (event) {
        case E_HUB_REQUEST:
            ret = MemQueueRequest_New(
                o->mod_state,
                (PyObject *)o,
                payload,
                D_FROM_MAIN,
                sub->channel,
                id
            );
            if (ret == NULL) {
                goto err;
            }
            goto done;

        case E_HUB_PUSH:
        case E_HUB_BROADCAST:
            PyErr_SetString(
                PyExc_RuntimeError,
                "unexpected event kind in listen()");
            goto err;

        default:
            Py_UNREACHABLE();
    }

done:
    assert(ret != NULL);
    Py_DECREF(payload);
    return ret;

err:
    Py_XDECREF(payload);
    Py_XDECREF(ret);
    return NULL;
}

static PyObject *
memhive_py_close_subs_health_queue(MemHive *o, PyObject *args)
{
    int ret = MemQueue_Close(&o->subs_health, o->mod_state);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_close_subs_queue(MemHive *o, PyObject *args)
{
    memhive_do_refs(o);
    int ret = MemQueue_Close(&o->for_subs, o->mod_state);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_close(MemHive *o, PyObject *args)
{
    memhive_do_refs(o);
    int ret = MemQueue_Close(&o->for_subs, o->mod_state);
    assert(ret == 0);
    ret = MemQueue_Close(&o->for_main, o->mod_state);
    assert(ret == 0);
    ret = MemQueue_Close(&o->subs_health, o->mod_state);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_do_refs(MemHive *o, PyObject *args)
{
    memhive_do_refs(o);
    Py_RETURN_NONE;
}


static PyMethodDef MemHive_methods[] = {
    {"broadcast", (PyCFunction)memhive_py_broadcast, METH_O, NULL},
    {"push", (PyCFunction)memhive_py_push, METH_O, NULL},
    {"listen", (PyCFunction)memhive_py_listen, METH_NOARGS, NULL},
    {"listen_subs_health", (PyCFunction)memhive_py_listen_subs_health,
        METH_NOARGS, NULL},
    {"close_subs_queue", (PyCFunction)memhive_py_close_subs_queue,
        METH_NOARGS, NULL},
    {"close_subs_health_queue", (PyCFunction)memhive_py_close_subs_health_queue,
        METH_NOARGS, NULL},
    {"close", (PyCFunction)memhive_py_close, METH_NOARGS, NULL},
    {"process_refs", (PyCFunction)memhive_py_do_refs, METH_NOARGS, NULL},
    {NULL, NULL}
};


PyType_Slot MemHive_TypeSlots[] = {
    {Py_tp_methods, MemHive_methods},
    {Py_mp_length, (lenfunc)memhive_tp_len},
    {Py_mp_subscript, (binaryfunc)memhive_tp_subscript},
    {Py_mp_ass_subscript, (objobjargproc)memhive_tp_ass_sub},
    {Py_sq_contains, (objobjproc)memhive_tp_contains},
    {Py_tp_init, (initproc)memhive_tp_init},
    {Py_tp_dealloc, (destructor)memhive_tp_dealloc},
    {0, NULL},
};


PyType_Spec MemHive_TypeSpec = {
    .name = "memhive.core.MemHive",
    .basicsize = sizeof(MemHive),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots = MemHive_TypeSlots,
};
