#include <stdint.h>
#include "memhive.h"


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
    module_state *state = PyType_GetModuleState(Py_TYPE(o));
    return MemHive_Get(state, (MemHive *)o->hive, key);
}


PyType_Slot MemHiveProxy_TypeSlots[] = {
    {Py_mp_length, (lenfunc)memhive_proxy_tp_len},
    {Py_mp_subscript, (binaryfunc)memhive_proxy_tp_subscript},
    {Py_tp_init, (initproc)memhive_proxy_tp_init},
    {Py_tp_dealloc, (destructor)memhive_proxy_tp_dealloc},
    {0, NULL},
};


PyType_Spec MemHiveProxy_TypeSpec = {
    .name = "memhive._MemHiveProxy",
    .basicsize = sizeof(MemHiveProxy),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = MemHiveProxy_TypeSlots,
};
