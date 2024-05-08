#include "memhive.h"
#include "module.h"
#include "queue.h"


static int
memhive_tp_init(MemHive *o, PyObject *args, PyObject *kwds)
{
    o->index = PyDict_New();
    if (o->index == NULL) {
        goto err;
    }

    if (pthread_rwlock_init(&o->index_rwlock, NULL)) {
        Py_FatalError("Failed to initialize an RWLock");
    }

    module_state *state = MemHive_GetModuleStateByPythonType(Py_TYPE(o));
    o->in = NewMemQueue(state);
    o->out = NewMemQueue(state);

    o->subs_list = NULL;
    if (pthread_mutex_init(&o->subs_list_mut, NULL)) {
        Py_FatalError("Failed to initialize a mutex");
    }

    return 0;

err:
    Py_CLEAR(o->index);
    return -1;
}

int
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
    pthread_mutex_unlock(&hive->subs_list_mut);
    return 0;
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

    PyObject_Del(o);
}


static Py_ssize_t
memhive_tp_len(MemHive *o)
{
    if (pthread_rwlock_rdlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    Py_ssize_t size = PyDict_Size(o->index);

    if (pthread_rwlock_unlock(&o->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    return size;
}


static PyObject *
memhive_tp_subscript(MemHive *o, PyObject *key)
{
    if (pthread_rwlock_rdlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    PyObject *val = NULL;
    if (PyDict_Contains(o->index, key)) {
        // An error besides KeyError should never happen, as we're
        // limiting the key type to a unicode object, which shouldn't ever
        // fail in its __eq__ or __hash__ methods. But if it does...
        // horrible things might happen, as we'd be remotely
        // inducing another interpreter into an error state *at a distance*.
        // Now this would be actually spooky.
        // ToDo: we should stop using the dict type for the index.
        val = PyDict_GetItemWithError(o->index, key);
    }

    if (pthread_rwlock_unlock(&o->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    if (val == NULL) {
        if (PyErr_Occurred()) {
            // See the above comment. If this happens we better know
            // this *can* happen, so please, dear sir, abandon the ship.
            Py_FatalError("Failed to lookup a key in the index");
        }
        else {
            PyErr_SetObject(PyExc_KeyError, key);
        }
    }

    Py_INCREF(val);
    return val;
}


static int
memhive_tp_ass_sub(MemHive *o, PyObject *key, PyObject *val)
{
    if (!MEMHIVE_IS_VALID_KEY(key)) {
        // we want to only allow primitive immutable objects as keys
        PyErr_SetString(
            PyExc_KeyError,
            "only primitive immutable objects are allowed");
        return -1;
    }

    if (!MEMHIVE_IS_PROXYABLE(val) && !MEMHIVE_IS_COPYABLE(val)) {
        // we want to only allow proxyable objects as values
        PyErr_SetString(
            PyExc_ValueError,
            "only proxyable/copyable objects are allowed");
        return -1;
    }

    if (pthread_rwlock_wrlock(&o->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index write lock");
    }

    int res = PyDict_SetItem(o->index, key, val);

    if (pthread_rwlock_unlock(&o->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index write lock");
    }

    return res;
}


Py_ssize_t
MemHive_Len(MemHive *hive)
{
    return memhive_tp_len(hive);
}

PyObject *
MemHive_Get(module_state *calling_state, MemHive *hive, PyObject *key)
{
    if (pthread_rwlock_rdlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    PyObject *val = NULL;
    if (PyDict_Contains(hive->index, key)) {
        val = PyDict_GetItemWithError(hive->index, key);
    }

    if (pthread_rwlock_unlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    if (val != NULL) {
        return MemHive_CopyObject(calling_state, val);
    } else {
        PyErr_SetObject(PyExc_KeyError, key);
    }

    return val;
}

static PyObject *
memhive_py_put(MemHive *o, PyObject *val)
{
    MemQueue *q = o->in;
    Py_INCREF(val);
    return MemQueue_Put(q, (PyObject*)o, val);
}

static PyObject *
memhive_py_get(MemHive *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);
    MemQueue *q = o->out;

    PyObject *sender;
    PyObject *remote_val;
    if (MemQueue_Get(q, state, &sender, &remote_val)) {
        return NULL;
    }

    if (MemHive_RefQueue_Dec(((MemHiveSub*)sender)->subs_refs, remote_val)) {
        return NULL;
    }

    return MemHive_CopyObject(state, remote_val);
}

static PyObject *
memhive_py_close_subs_intake(MemHive *o, PyObject *args)
{
    int ret = MemQueue_Close(o->in);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyObject *
memhive_py_do_refs(MemHive *o, PyObject *args)
{
    pthread_mutex_lock(&o->subs_list_mut);

    SubsList *lst = o->subs_list;
    while (lst != NULL) {
        if (MemHive_RefQueue_Run(lst->sub->main_refs)) {
            pthread_mutex_unlock(&o->subs_list_mut);
            return NULL;
        }
        lst = lst->next;
    }

    pthread_mutex_unlock(&o->subs_list_mut);

    Py_RETURN_NONE;
}


static PyMethodDef MemHive_methods[] = {
    {"put", (PyCFunction)memhive_py_put, METH_O, NULL},
    {"get", (PyCFunction)memhive_py_get, METH_NOARGS, NULL},
    {"close_subs_intake", (PyCFunction)memhive_py_close_subs_intake, METH_NOARGS, NULL},
    {"do_refs", (PyCFunction)memhive_py_do_refs, METH_NOARGS, NULL},
    {NULL, NULL}
};


PyType_Slot MemHive_TypeSlots[] = {
    {Py_tp_methods, MemHive_methods},
    {Py_mp_length, (lenfunc)memhive_tp_len},
    {Py_mp_subscript, (binaryfunc)memhive_tp_subscript},
    {Py_mp_ass_subscript, (objobjargproc)memhive_tp_ass_sub},
    {Py_tp_init, (initproc)memhive_tp_init},
    {Py_tp_dealloc, (destructor)memhive_tp_dealloc},
    {0, NULL},
};


PyType_Spec MemHive_TypeSpec = {
    .name = "_memhive._MemHive",
    .basicsize = sizeof(MemHive),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots = MemHive_TypeSlots,
};
