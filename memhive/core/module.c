#include "memhive.h"
#include "queue.h"
#include "map.h"
#include "debug.h"
#include "errormech.h"

static struct PyModuleDef memhive_module;


static int module_exec(PyObject *m);

#ifdef DEBUG
static PyObject * enable_object_tracking(PyObject *arg);
static PyObject * disable_object_tracking(PyObject *arg);
#endif


static PyObject *
dump_error(PyObject *self, PyObject *err)
{
    return MemHive_DumpError(err);
}

static PyObject *
restore_error(PyObject *self, PyObject *err)
{
    module_state *state = MemHive_GetModuleState(self);
    return MemHive_RestoreError(state, (RemoteObject*)err);
}


static struct PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, module_exec},
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
    {0, NULL},
};


static PyMethodDef module_methods[] = {
#ifdef DEBUG
    {"enable_object_tracking", (PyCFunction)enable_object_tracking, METH_NOARGS, NULL},
    {"disable_object_tracking", (PyCFunction)disable_object_tracking, METH_NOARGS, NULL},
    {"dump_error", (PyCFunction)dump_error, METH_O, NULL},
    {"restore_error", (PyCFunction)restore_error, METH_O, NULL},
#endif
    {NULL, NULL}
};


module_state *
MemHive_GetModuleState(PyObject *mod)
{
    module_state *state = PyModule_GetState(mod);
    assert(state != NULL);
    return state;
}


module_state *
MemHive_GetModuleStateByType(PyTypeObject *cls)
{
    module_state *state = PyType_GetModuleState(cls);
    assert(state != NULL);
    return state;
}


module_state *
MemHive_GetModuleStateByPythonType(PyTypeObject *cls)
{
    PyObject *module = PyType_GetModuleByDef(cls, &memhive_module);
    assert(module != NULL);
    module_state *state = PyModule_GetState(module);
    assert(state != NULL);
    return state;
}


module_state *
MemHive_GetModuleStateByObj(PyObject *obj)
{
    module_state *state = PyType_GetModuleState(Py_TYPE(obj));
    assert(state != NULL);
    return state;
}


static int
module_clear(PyObject *mod)
{
    module_state *state = PyModule_GetState(mod);

    Py_CLEAR(state->ClosedQueueError);

    Py_CLEAR(state->MemHive_Type);
    Py_CLEAR(state->MemHiveSub_Type);

    Py_CLEAR(state->MapType);
    Py_CLEAR(state->MapMutationType);

    Py_CLEAR(state->ArrayNodeType);
    Py_CLEAR(state->BitmapNodeType);
    Py_CLEAR(state->CollisionNodeType);

    Py_CLEAR(state->MapKeysType);
    Py_CLEAR(state->MapValuesType);
    Py_CLEAR(state->MapItemsType);

    Py_CLEAR(state->MapKeysIterType);
    Py_CLEAR(state->MapValuesIterType);
    Py_CLEAR(state->MapItemsIterType);

    Py_CLEAR(state->MemQueueRequestType);
    Py_CLEAR(state->MemQueueResponseType);
    Py_CLEAR(state->MemQueueBroadcastType);

    Py_CLEAR(state->sub);

    Py_CLEAR(state->exc_empty_dict);
    Py_CLEAR(state->exc_types_cache);
    Py_CLEAR(state->exc_frames_cache);

    Py_CLEAR(state->str_ERROR);
    Py_CLEAR(state->str_START);
    Py_CLEAR(state->str_CLOSE);

    #ifdef DEBUG
    Py_CLEAR(state->debug_objects_ids);
    #endif

    PyMem_RawFree(state->proxy_desc_template);
    state->proxy_desc_template = NULL;

    return 0;
}

static void
module_free(PyObject *module)
{
    (void)module_clear(module);
}


static int
module_traverse(PyObject *mod, visitproc visit, void *arg)
{
    module_state *state = MemHive_GetModuleState(mod);

    Py_VISIT(state->ClosedQueueError);

    Py_VISIT(state->MemHive_Type);
    Py_VISIT(state->MemHiveSub_Type);

    Py_VISIT(state->MapType);
    Py_VISIT(state->MapMutationType);

    Py_VISIT(state->ArrayNodeType);
    Py_VISIT(state->BitmapNodeType);
    Py_VISIT(state->CollisionNodeType);

    Py_VISIT(state->MapKeysType);
    Py_VISIT(state->MapValuesType);
    Py_VISIT(state->MapItemsType);

    Py_VISIT(state->MapKeysIterType);
    Py_VISIT(state->MapValuesIterType);
    Py_VISIT(state->MapItemsIterType);

    Py_VISIT(state->MemQueueRequestType);
    Py_VISIT(state->MemQueueResponseType);
    Py_VISIT(state->MemQueueBroadcastType);

    Py_VISIT(state->sub);

    Py_VISIT(state->exc_empty_dict);
    Py_VISIT(state->exc_types_cache);
    Py_VISIT(state->exc_frames_cache);

    Py_VISIT(state->str_ERROR);
    Py_VISIT(state->str_START);
    Py_VISIT(state->str_CLOSE);

    #ifdef DEBUG
    Py_VISIT(state->debug_objects_ids);
    #endif

    return 0;
}


#ifdef DEBUG
static PyObject *
enable_object_tracking(PyObject *m)
{
    module_state *state = MemHive_GetModuleState(m);
    state->debug_tracking = 1;
    Py_RETURN_NONE;
}

static PyObject *
disable_object_tracking(PyObject *m)
{
    module_state *state = MemHive_GetModuleState(m);
    state->debug_tracking = 0;
    if (PySet_Clear(state->debug_objects_ids)) abort();
    Py_RETURN_NONE;
}
#endif


static struct PyModuleDef memhive_module = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "memhive.core._core",
    .m_doc = NULL,
    .m_size = sizeof(module_state),
    .m_methods = module_methods,
    .m_slots = module_slots,
    .m_traverse = module_traverse,
    .m_clear = module_clear,
    .m_free = (freefunc)module_free,
};


static int
module_exec(PyObject *m)
{
    module_state *state = MemHive_GetModuleState(m);

    #ifdef DEBUG
    state->debug_tracking = 0;
    state->debug_objects_ids = PySet_New(NULL);
    assert(state->debug_objects_ids != NULL);
    #endif

    #define CREATE_TYPE(mod, tp, spec, base, exp)                       \
    do {                                                                \
        tp = (PyTypeObject *)PyType_FromMetaclass(                      \
            NULL, mod, spec, (PyObject *)base);                         \
        if (tp == NULL || PyType_Ready(tp) < 0) {                       \
            return -1;                                                  \
        }                                                               \
        if (exp && (PyModule_AddType(mod, tp) < 0)) {                   \
            return -1;                                                  \
        }                                                               \
    } while (0)

    #define CREATE_EXC(mod, tp, name, base, exp)                        \
    do {                                                                \
        tp = PyErr_NewException("memhive." name, base, NULL);           \
        if (tp == NULL) {                                               \
            return -1;                                                  \
        }                                                               \
        if (exp && PyModule_AddObjectRef(mod, name, tp) < 0) {          \
            return -1;                                                  \
        }                                                               \
    } while (0)

    CREATE_EXC(m, state->ClosedQueueError, "ClosedQueueError", PyExc_Exception, 1);

    CREATE_TYPE(m, state->MemHive_Type, &MemHive_TypeSpec, NULL, 1);
    CREATE_TYPE(m, state->MemHiveSub_Type, &MemHiveSub_TypeSpec, NULL, 1);

    CREATE_TYPE(m, state->MapType, &Map_TypeSpec, NULL, 1);
    CREATE_TYPE(m, state->MapMutationType, &MapMutation_TypeSpec, NULL, 0);

    CREATE_TYPE(m, state->ArrayNodeType, &ArrayNode_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->BitmapNodeType, &BitmapNode_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->CollisionNodeType, &CollisionNode_TypeSpec, NULL, 0);

    CREATE_TYPE(m, state->MapKeysType, &MapKeys_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->MapValuesType, &MapValues_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->MapItemsType, &MapItems_TypeSpec, NULL, 0);

    CREATE_TYPE(m, state->MapKeysIterType, &MapKeysIter_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->MapValuesIterType, &MapValuesIter_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->MapItemsIterType, &MapItemsIter_TypeSpec, NULL, 0);

    CREATE_TYPE(m, state->MemQueueRequestType,
                &MemQueueRequest_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->MemQueueResponseType,
                &MemQueueResponse_TypeSpec, NULL, 0);
    CREATE_TYPE(m, state->MemQueueBroadcastType,
                &MemQueueBroadcast_TypeSpec, NULL, 0);

    state->str_ERROR = PyUnicode_FromString("ERROR");
    if (state->str_ERROR == NULL) {
        return -1;
    }
    state->str_START = PyUnicode_FromString("START");
    if (state->str_START == NULL) {
        return -1;
    }
    state->str_CLOSE = PyUnicode_FromString("CLOSE");
    if (state->str_CLOSE == NULL) {
        return -1;
    }

    PyInterpreterState *interp = PyInterpreterState_Get();
    assert(interp != NULL);
    state->interpreter_id = PyInterpreterState_GetID(interp);

    state->sub = NULL; // will be initialized later, in MemHiveSub's __init__

    ProxyDescriptor *proxy_desc = PyMem_RawMalloc(sizeof(ProxyDescriptor));
    if (proxy_desc == NULL) {
        return -1;
    }
    // TODO: add an indirection function looking at type name and
    // dispatching accordingly. Will only need that when we add the
    // tuple-like type to compliment our mapping.
    proxy_desc->copy_from_main_to_sub = MemHive_NewMapProxy;
    proxy_desc->copy_from_sub_to_main = MemHive_CopyMapProxy;
    state->proxy_desc_template = proxy_desc;

    state->exc_empty_dict = PyDict_New();
    if (state->exc_empty_dict == NULL) {
        return -1;
    }
    state->exc_frames_cache = PyDict_New();
    if (state->exc_frames_cache == NULL) {
        return -1;
    }
    state->exc_types_cache = PyDict_New();
    if (state->exc_types_cache == NULL) {
        return -1;
    }

    return 0;
}


PyMODINIT_FUNC
PyInit__core(void)
{
    return PyModuleDef_Init(&memhive_module);
}
