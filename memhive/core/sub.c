#include <stdint.h>

#include "memhive.h"
#include "errormech.h"
#include "module.h"
#include "track.h"
#include "queue.h"
#include "debug.h"


static int
memhive_sub_tp_init(MemHiveSub *o, PyObject *args, PyObject *kwds)
{
    uintptr_t hive_ptr;
    uint64_t sub_id;

    if (!PyArg_ParseTuple(args, "KK", &hive_ptr, &sub_id)) {
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

    o->sub_id = sub_id;

    o->hive = (RemoteObject*)hive_ptr;
    module_state *state = MemHive_GetModuleStateByPythonType(Py_TYPE(o));

    ssize_t channel = MemHive_RegisterSub((MemHive*)o->hive, o, state);
    if (channel < 0) {
        return -1;
    }

    if (MemHive_RefQueue_Inc(o->main_refs, o->hive)) {
        // Really this can only fail with NoMemory, but still
        MemHive_UnregisterSub((MemHive*)o->hive, o);
        return -1;
    }

    o->channel = channel;

    state->sub = (PyObject*)o;
    Py_INCREF(state->sub);

    o->closed = 0;
    o->req_id_cnt = 0;

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
    return PyObject_Length((PyObject *)(o->hive));
}


static void
memhive_sub_tp_dealloc(MemHiveSub *o)
{
    PyTypeObject *tp = Py_TYPE(o);
    tp->tp_free((PyObject *)o);
    Py_DecRef((PyObject*)tp);
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
    RemoteObject *sender;
    RemoteObject *remote_val;
    uint64_t id;

    if (MemQueue_Listen(q, state, o->channel,
                        &event, &sender, &id, &remote_val))
    {
        return NULL;
    }

    PyObject *ret = NULL;

    PyObject *payload = MemHive_CopyObject(state, remote_val);
    if (payload == NULL) {
        goto err;
    }

    if (MemHive_RefQueue_Dec(o->main_refs, remote_val)) {
        goto err;
    }

    switch (event) {
        case E_HUB_PUSH:
            ret = MemQueueRequest_New(
                state,
                (PyObject *)o,
                payload,
                D_FROM_SUB,
                0,
                id
            );
            break;

        case E_HUB_BROADCAST:
            ret = MemQueueBroadcast_New(state, payload);
            break;

        case E_HUB_REQUEST:
            ret = MemQueueResponse_New(
                state,
                payload,
                NULL,
                id
            );
            break;

        default:
            Py_UNREACHABLE();
    }

    if (ret == NULL) {
        goto err;
    }

    Py_DECREF(payload);
    return ret;

err:
    Py_XDECREF(payload);
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
    if (MemQueue_HubRequest(q, state, 0, (PyObject*)o, ++o->req_id_cnt, arg)) {
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

static PyObject *
memhive_sub_py_report_start(MemHiveSub *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);

    int ret = MemQueue_Put(
        &((MemHive*)o->hive)->subs_health,
        state,
        E_HEALTH_START,
        0,
        (PyObject*)o,
        o->sub_id,
        NULL
    );

    if (ret) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
memhive_sub_py_report_close(MemHiveSub *o, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);

    int ret = MemQueue_Put(
        &((MemHive*)o->hive)->subs_health,
        state,
        E_HEALTH_CLOSE,
        0,
        (PyObject*)o,
        o->sub_id,
        NULL
    );

    if (ret) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
memhive_sub_py_report_error(MemHiveSub *o, PyObject *args)
{
    PyObject *exc_name;
    PyObject *exc_msg;
    PyObject *cause;

    if (!PyArg_UnpackTuple(args, "report_error", 3, 3,
                           &exc_name, &exc_msg, &cause))
    {
        return NULL;
    }

    PyObject * ser_err = MemHive_DumpError(cause);
    if (ser_err == NULL) {
        return NULL;
    }

    PyObject *res = PyTuple_Pack(3, exc_name, exc_msg, ser_err);
    Py_CLEAR(ser_err);
    if (res == NULL) {
        return NULL;
    }

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);

    int ret = MemQueue_Put(
        &((MemHive*)o->hive)->subs_health,
        state,
        E_HEALTH_ERROR,
        0,
        (PyObject*)o,
        o->sub_id,
        res
    );
    Py_CLEAR(ser_err);

    if (ret) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyMethodDef MemHiveSub_methods[] = {
    {"request", (PyCFunction)memhive_sub_py_request, METH_O, NULL},
    {"listen", (PyCFunction)memhive_sub_py_listen, METH_NOARGS, NULL},
    {"process_refs", (PyCFunction)memhive_sub_py_do_refs, METH_NOARGS, NULL},
    {"close", (PyCFunction)memhive_sub_py_close, METH_NOARGS, NULL},
    {"report_error", (PyCFunction)memhive_sub_py_report_error,
        METH_VARARGS, NULL},
    {"report_start", (PyCFunction)memhive_sub_py_report_start,
        METH_NOARGS, NULL},
    {"report_close", (PyCFunction)memhive_sub_py_report_close,
        METH_NOARGS, NULL},
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
