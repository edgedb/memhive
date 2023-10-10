#include <stdint.h>
#include "memhive.h"


static MemHiveProxy *
memhive_proxy_tp_new(void)
{
    MemHiveProxy *o;

    o = PyObject_New(MemHiveProxy, &MemHiveProxy_Type);
    if (o == NULL) {
        return NULL;
    }

    return o;
}


static int
memhive_proxy_tp_init(MemHiveProxy *o, PyObject *args, PyObject *kwds)
{
    uintptr_t hive_ptr;
    // Parse the Python int object as a uintptr_t
    if (!PyArg_ParseTuple(args, "K", &hive_ptr)) {
        return -1;
    }

    o->hive = (DistantPyObject*)hive_ptr;

    return 0;
}


static Py_ssize_t
memhive_proxy_tp_len(MemHiveProxy *o)
{
    // It's safe to do this because MemHive's tp_len is protected by a mutex.
    return PyObject_Length(o->hive);
}


static void
memhive_proxy_tp_dealloc(MemHiveProxy *o)
{
    PyObject_Del(o);
}

static PyObject *
memhive_proxy_tp_subscript(MemHiveProxy *o, PyObject *key)
{
    return MemHive_Get(o->hive, key);
}


static PyMappingMethods MemHiveProxy_as_mapping = {
    (lenfunc)memhive_proxy_tp_len,             /* mp_length */
    (binaryfunc)memhive_proxy_tp_subscript,    /* mp_subscript */
};


PyTypeObject MemHiveProxy_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "memhive._MemHiveProxy",

    .tp_basicsize = sizeof(MemHiveProxy),
    .tp_itemsize = 0,

    .tp_as_mapping = &MemHiveProxy_as_mapping,

    .tp_getattro = PyObject_GenericGetAttr,

    // this is a non-GC object
    .tp_flags = Py_TPFLAGS_DEFAULT,

    .tp_new = (newfunc)memhive_proxy_tp_new,
    .tp_init = (initproc)memhive_proxy_tp_init,
    .tp_dealloc = (destructor)memhive_proxy_tp_dealloc,
};
