#include "memhive.h"


static MemHive *
memhive_tp_new(void)
{
    MemHive *o;
    o = PyObject_New(MemHive, &MemHive_Type);
    if (o == NULL) {
        return NULL;
    }

    o->index = NULL;

    o->index = PyDict_New();
    if (o->index == NULL) {
        goto err;
    }

    if (pthread_rwlock_init(&o->index_rwlock, NULL)) {
        Py_FatalError("Failed to initialize an RWLock");
    }

    return o;

err:
    Py_CLEAR(o->index);
    Py_CLEAR(o);
    return NULL;
}


static int
memhive_tp_init(MemHive *o, PyObject *args, PyObject *kwds)
{
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
MemHive_Get(MemHive *hive, PyObject *key)
{
    if (pthread_rwlock_rdlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to acquire the MemHive index read lock");
    }

    PyObject *val = NULL;
    if (PyDict_Contains(hive->index, key)) {
        val = PyDict_GetItemWithError(hive->index, key);
    }

    PyObject *mirrored = NULL;
    if (val != NULL) {
        mirrored = MemHive_CopyObject(val);
        if (mirrored == NULL) {
            return NULL;
        }
    }

    if (pthread_rwlock_unlock(&hive->index_rwlock)) {
        Py_FatalError("Failed to release the MemHive index read lock");
    }

    if (val == NULL) {
        PyErr_SetObject(PyExc_KeyError, key);
    }

    return val;
}



static PyMappingMethods MemHive_as_mapping = {
    (lenfunc)memhive_tp_len,                 /* mp_length */
    (binaryfunc)memhive_tp_subscript,        /* mp_subscript */
    (objobjargproc)memhive_tp_ass_sub,       /* mp_ass_subscript */
};


PyTypeObject MemHive_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "memhive._MemHive",

    .tp_basicsize = sizeof(MemHive),
    .tp_itemsize = 0,

    .tp_as_mapping = &MemHive_as_mapping,

    .tp_getattro = PyObject_GenericGetAttr,

    // this is a non-GC object
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,

    .tp_new = (newfunc)memhive_tp_new,
    .tp_init = (initproc)memhive_tp_init,
    .tp_dealloc = (destructor)memhive_tp_dealloc,
};
