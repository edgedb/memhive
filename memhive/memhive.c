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

    return 0;

err:
    Py_CLEAR(o->index);
    return -1;
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
memhive_py_put(MemHive *o, PyObject *borrowed_val)
{
    MemQueue *q = o->in;
    return MemQueue_Put(q, borrowed_val);
}

static PyObject *
memhive_py_get_proxy(MemHive *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);
    MemQueue *q = o->out;
    return MemQueue_GetAndProxy(q, state);
}

static PyObject *
memhive_py_close_subs_intake(MemHive *o, PyObject *args)
{
    int ret = MemQueue_Close(o->in);
    assert(ret == 0);
    Py_RETURN_NONE;
}

static PyMethodDef MemHive_methods[] = {
    {"put_borrowed", (PyCFunction)memhive_py_put, METH_O, NULL},
    {"get_proxied", (PyCFunction)memhive_py_get_proxy, METH_NOARGS, NULL},
    {"close_subs_intake", (PyCFunction)memhive_py_close_subs_intake, METH_NOARGS, NULL},
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
