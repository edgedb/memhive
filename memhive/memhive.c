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


static PyMappingMethods MemHive_as_mapping = {
    (lenfunc)memhive_tp_len,             /* mp_length */
    //(binaryfunc)map_tp_subscript,    /* mp_subscript */
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
