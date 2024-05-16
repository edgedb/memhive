#include <stdint.h>

#include "memhive.h"
#include "module.h"
#include "track.h"
#include "queue.h"


static int
memhive_sub_tp_init(MemHiveSub *o, PyObject *args, PyObject *kwds)
{
    uintptr_t hive_ptr;
    // Parse the Python int object as a uintptr_t
    if (!PyArg_ParseTuple(args, "K", &hive_ptr)) {
        return -1;
    }

    o->main_refs = MemHive_RefQueue_New();
    if (o->main_refs == NULL) {
        return -1;
    }

    o->subs_refs = MemHive_RefQueue_New();
    if (o->subs_refs == NULL) {
        return -1;
    }

    o->hive = (DistantPyObject*)hive_ptr;
    module_state *state = MemHive_GetModuleStateByPythonType(Py_TYPE(o));

    ssize_t channel = MemHive_RegisterSub((MemHive*)o->hive, o, state);
    if (channel < 0) {
        return -1;
    }

    if (MemHive_RefQueue_Inc(o->main_refs, o->hive)) {
        return -1;
    }

    o->channel = channel;

    state->sub = (PyObject*)o;
    Py_INCREF(state->sub);

    o->closed = 0;

    TRACK(state, o);

    return 0;
}


static int
memhive_ensure_open(MemHiveSub *o)
{
    if (o->closed) {
        PyErr_SetString(PyExc_ValueError, "subinterpreter is closing");
        return -1;
    }
    return 0;
}


static Py_ssize_t
memhive_sub_tp_len(MemHiveSub *o)
{
    // It's safe to do this because MemHive's tp_len is protected by a mutex.
    if (memhive_ensure_open(o)) {
        return -1;
    }
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
    if (memhive_ensure_open(o)) {
        return NULL;
    }
    module_state *state = PyType_GetModuleState(Py_TYPE(o));
    return MemHive_Get(state, (MemHive *)o->hive, key);
}

static int
memhive_sub_tp_contains(MemHiveSub *o, PyObject *key)
{
    if (memhive_ensure_open(o)) {
        return -1;
    }
    module_state *state = PyType_GetModuleState(Py_TYPE(o));
    return MemHive_Contains(state, (MemHive *)o->hive, key);
}

static PyObject *
memhive_sub_py_listen(MemHiveSub *o, PyObject *args)
{
    if (memhive_ensure_open(o)) {
        return NULL;
    }

    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);
    MemQueue *q = &((MemHive *)o->hive)->for_subs;

    memqueue_event_t event;
    PyObject *sender;
    PyObject *remote_val;

    if (MemQueue_Listen(q, state, o->channel, &event, &sender, &remote_val)) {
        return NULL;
    }

    PyObject *ret = NULL;
    PyObject *resp = NULL;

    PyObject *payload = MemHive_CopyObject(state, remote_val);
    if (payload == NULL) {
        goto err;
    }

    if (MemHive_RefQueue_Dec(o->main_refs, remote_val)) {
        goto err;
    }

    if (event == E_PUSH) {
        resp = MemQueueReplyCallback_New(
            state, (PyObject *)o, D_FROM_SUB, 0, E_PUSH);
        if (resp == NULL) {
            goto err;
        }
    }

    ret = MemQueueMessage_New(state, event, payload, resp);
    if (ret == NULL) {
        goto err;
    }
    Py_CLEAR(payload);
    Py_CLEAR(resp);

    return ret;

err:
    Py_XDECREF(payload);
    Py_XDECREF(resp);
    Py_XDECREF(ret);
    return NULL;
}

static PyObject *
memhive_sub_py_request(MemHiveSub *o, PyObject *arg)
{
    if (memhive_ensure_open(o)) {
        return NULL;
    }
    MemQueue *q = &((MemHive *)o->hive)->for_main;
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);
    TRACK(state, arg);
    if (MemQueue_Request(q, state, 0, (PyObject*)o, arg)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
memhive_sub_py_do_refs(MemHiveSub *o, PyObject *args)
{
    if (memhive_ensure_open(o)) {
        return NULL;
    }
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);
    MemHive_RefQueue_Run(o->subs_refs, state);
    Py_RETURN_NONE;
}

static PyObject *
memhive_sub_py_close(MemHiveSub *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);
    if (o->closed) {
        Py_RETURN_NONE;
    }
    o->closed = 1;
    if (MemHive_RefQueue_Dec(o->main_refs, o->hive)) {
        return NULL;
    }
    MemHive_RefQueue_Run(o->subs_refs, state);
    MemHive_UnregisterSub((MemHive*)o->hive, o);
    Py_RETURN_NONE;
}

static PyMethodDef MemHiveSub_methods[] = {
    {"request", (PyCFunction)memhive_sub_py_request, METH_O, NULL},
    {"listen", (PyCFunction)memhive_sub_py_listen, METH_NOARGS, NULL},
    {"process_refs", (PyCFunction)memhive_sub_py_do_refs, METH_NOARGS, NULL},
    {"close", (PyCFunction)memhive_sub_py_close, METH_NOARGS, NULL},
    {NULL, NULL}
};


PyType_Slot MemHiveSub_TypeSlots[] = {
    {Py_tp_methods, MemHiveSub_methods},
    {Py_mp_length, (lenfunc)memhive_sub_tp_len},
    {Py_mp_subscript, (binaryfunc)memhive_sub_tp_subscript},
    {Py_sq_contains, (objobjproc)memhive_sub_tp_contains},
    {Py_tp_init, (initproc)memhive_sub_tp_init},
    {Py_tp_dealloc, (destructor)memhive_sub_tp_dealloc},
    {0, NULL},
};


PyType_Spec MemHiveSub_TypeSpec = {
    .name = "memhive.core.MemHiveSub",
    .basicsize = sizeof(MemHiveSub),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = MemHiveSub_TypeSlots,
};
