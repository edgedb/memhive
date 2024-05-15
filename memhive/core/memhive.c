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

    if (MemQueue_Init(&o->for_subs)) {
        Py_FatalError("Failed to initialize the subs intake queue");
    }
    if (MemQueue_Init(&o->for_main)) {
        Py_FatalError("Failed to initialize the subs output queue");
    }

    o->subs_list = NULL;
    if (pthread_mutex_init(&o->subs_list_mut, NULL)) {
        Py_FatalError("Failed to initialize a mutex");
    }

    TRACK(state, o);

    return 0;

err:
    Py_CLEAR(o->index);
    return -1;
}

ssize_t
MemHive_RegisterSub(MemHive *hive, MemHiveSub *sub) {
    SubsList *cnt = malloc(sizeof (SubsList));
    if (cnt == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    cnt->sub = sub;
    cnt->next = NULL;
    pthread_mutex_lock(&hive->subs_list_mut);
    if (hive->subs_list == NULL) {
        hive->subs_list = cnt;
    } else {
        cnt->next = hive->subs_list;
        hive->subs_list = cnt;
    }

    ssize_t channel = MemQueue_AddChannel(&hive->for_subs);
    pthread_mutex_unlock(&hive->subs_list_mut);

    return channel;
}

static void
memhive_do_refs(MemHive *hive, module_state *state)
{
    pthread_mutex_lock(&hive->subs_list_mut);

    SubsList *lst = hive->subs_list;
    while (lst != NULL) {
        MemHive_RefQueue_Run(lst->sub->main_refs, state);
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
        free(l);
        l = next;
    }
    o->subs_list = NULL;

    MemQueue_Destroy(&o->for_main);
    MemQueue_Destroy(&o->for_subs);

    PyObject_Del(o);
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
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);
    return MemHive_Get(state, o, key);
}


static int
memhive_tp_ass_sub(MemHive *o, PyObject *key, PyObject *val)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);

    if (pthread_rwlock_wrlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index write lock");
    }

    // It's important to do the refs here! When a remote interpreter reads
    // a value, say another Map, that Map has to keep existing in the main
    // interpreter. All scheduled increfs while `index_rwlock` *read* lock
    // was held must be applied before we apply changes while having
    // the *write* lock.
    memhive_do_refs(o, state);

    PyObject *new_index = MemHive_MapSetItem(state, o->index, key, val);
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
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)hive);
    return MemHive_MapContains(state, hive->index, key);
}


Py_ssize_t
MemHive_Len(MemHive *hive)
{
    return memhive_tp_len(hive);
}

PyObject *
MemHive_Get(module_state *state, MemHive *hive, PyObject *key)
{
    if (pthread_rwlock_rdlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    PyObject *val = MemHive_MapGetItem(state, hive->index, key, NULL);

    if (pthread_rwlock_unlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    Py_XINCREF(val);
    return val;
}

int
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
    #ifdef DEBUG
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);
    #endif

    TRACK(state, val);
    if (MemQueue_Push(&o->for_subs, 0, (PyObject*)o, val)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_get(MemHive *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);

    memqueue_event_t event;
    PyObject *sender;
    PyObject *remote_val;
    if (MemQueue_Listen(&o->for_main, state, 0, &event, &sender, &remote_val)) {
        return NULL;
    }

    PyObject *ret = MemHive_CopyObject(state, remote_val);
    if (ret == NULL) {
        return NULL;
    }

    if (MemHive_RefQueue_Dec(((MemHiveSub*)sender)->subs_refs, remote_val)) {
        return NULL;
    }

    return ret;
}

static PyObject *
memhive_py_close_subs_intake(MemHive *o, PyObject *args)
{
    int ret = MemQueue_Close(&o->for_subs);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_close(MemHive *o, PyObject *args)
{
    int ret = MemQueue_Close(&o->for_subs);
    assert(ret == 0);
    ret = MemQueue_Close(&o->for_main);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_do_refs(MemHive *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);
    memhive_do_refs(o, state);
    Py_RETURN_NONE;
}


static PyMethodDef MemHive_methods[] = {
    {"push", (PyCFunction)memhive_py_push, METH_O, NULL},
    {"get", (PyCFunction)memhive_py_get, METH_NOARGS, NULL},
    {"close_subs_intake", (PyCFunction)memhive_py_close_subs_intake, METH_NOARGS, NULL},
    {"close", (PyCFunction)memhive_py_close, METH_NOARGS, NULL},
    {"do_refs", (PyCFunction)memhive_py_do_refs, METH_NOARGS, NULL},
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
