#include <stdint.h>
#include "memhive.h"


static int
memhive_sub_tp_init(MemHiveSub *o, PyObject *args, PyObject *kwds)
{
    uintptr_t hive_ptr;
    // Parse the Python int object as a uintptr_t
    if (!PyArg_ParseTuple(args, "K", &hive_ptr)) {
        return -1;
    }

    o->hive = (DistantPyObject*)hive_ptr;
    MemHive_RegisterSub((MemHive*)o->hive, o);

    return 0;
}


static Py_ssize_t
memhive_sub_tp_len(MemHiveSub *o)
{
    // It's safe to do this because MemHive's tp_len is protected by a mutex.
    return PyObject_Length(o->hive);
}


static void
memhive_sub_tp_dealloc(MemHiveSub *o)
{
    PyObject_Del(o);
}

static PyObject *
memhive_sub_tp_subscript(MemHiveSub *o, PyObject *key)
{
    module_state *state = PyType_GetModuleState(Py_TYPE(o));
    return MemHive_Get(state, (MemHive *)o->hive, key);
}


static PyObject *
memhive_sub_py_put(MemHiveSub *o, PyObject *borrowed_val)
{
    MemQueue *q = ((MemHive *)o->hive)->out;
    return MemQueue_Put(q, borrowed_val);
}

static PyObject *
memhive_sub_py_get_proxy(MemHiveSub *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);
    MemQueue *q = ((MemHive *)o->hive)->in;
    return MemQueue_GetAndProxy(q, state);
}


static PyMethodDef MemHiveSub_methods[] = {
    {"put_borrowed", (PyCFunction)memhive_sub_py_put, METH_O, NULL},
    {"get_proxied", (PyCFunction)memhive_sub_py_get_proxy, METH_NOARGS, NULL},
    {NULL, NULL}
};


PyType_Slot MemHiveSub_TypeSlots[] = {
    {Py_tp_methods, MemHiveSub_methods},
    {Py_mp_length, (lenfunc)memhive_sub_tp_len},
    {Py_mp_subscript, (binaryfunc)memhive_sub_tp_subscript},
    {Py_tp_init, (initproc)memhive_sub_tp_init},
    {Py_tp_dealloc, (destructor)memhive_sub_tp_dealloc},
    {0, NULL},
};


PyType_Spec MemHiveSub_TypeSpec = {
    .name = "memhive._MemHiveSub",
    .basicsize = sizeof(MemHiveSub),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = MemHiveSub_TypeSlots,
};
