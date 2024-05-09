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

    o->main_refs = MemHive_RefQueue_New();
    if (o->main_refs == NULL) {
        return -1;
    }

    o->subs_refs = MemHive_RefQueue_New();
    if (o->subs_refs == NULL) {
        return -1;
    }

    module_state *state = MemHive_GetModuleStateByPythonType(Py_TYPE(o));
    state->sub = (PyObject*)o;
    Py_INCREF(state->sub);

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
memhive_sub_py_put(MemHiveSub *o, PyObject *val)
{
    MemQueue *q = ((MemHive *)o->hive)->out;
    Py_INCREF(val);
    return MemQueue_Put(q, (PyObject*)o, val);
}

static PyObject *
memhive_sub_py_get(MemHiveSub *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);
    MemQueue *q = ((MemHive *)o->hive)->in;

    PyObject *sender;
    PyObject *remote_val;

    if (MemQueue_Get(q, state, &sender, &remote_val)) {
        return NULL;
    }

    PyObject *ret = MemHive_CopyObject(state, remote_val);
    if (ret == NULL) {
        return NULL;
    }

    if (MemHive_RefQueue_Dec(o->main_refs, remote_val)) {
        return NULL;
    }

    return ret;
}


static PyObject *
memhive_sub_py_do_refs(MemHiveSub *o, PyObject *args)
{
    if (MemHive_RefQueue_Run(o->subs_refs)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyMethodDef MemHiveSub_methods[] = {
    {"put", (PyCFunction)memhive_sub_py_put, METH_O, NULL},
    {"get", (PyCFunction)memhive_sub_py_get, METH_NOARGS, NULL},
    {"do_refs", (PyCFunction)memhive_sub_py_do_refs, METH_NOARGS, NULL},
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
