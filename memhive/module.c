#include "memhive.h"
#include "map.h"


static struct PyModuleDef memhive_module;


static int module_exec(PyObject *m);


static struct PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, module_exec},
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
    {0, NULL},
};


static PyMethodDef module_methods[] = {
    {NULL, NULL}
};


static module_state *
get_module_state(PyObject *mod)
{
    module_state *state = PyModule_GetState(mod);
    assert(state != NULL);
    return state;
}


static int
module_clear(PyObject *mod)
{
    module_state *state = PyModule_GetState(mod);

    Py_CLEAR(state->MemHive_Type);
    Py_CLEAR(state->MemHiveProxy_Type);

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

    Py_CLEAR(state->empty_bitmap_node);

    return 0;
}


static int
module_traverse(PyObject *mod, visitproc visit, void *arg)
{
    module_state *state = get_module_state(mod);

    Py_VISIT(state->MemHive_Type);
    Py_VISIT(state->MemHiveProxy_Type);

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

    return 0;
}


static struct PyModuleDef memhive_module = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "_memhive",
    .m_doc = NULL,
    .m_size = sizeof(module_state),
    .m_methods = module_methods,
    .m_slots = module_slots,
    .m_traverse = module_traverse,
    .m_clear = module_clear,
    .m_free = NULL,
};


static int
module_exec(PyObject *m)
{
    module_state *state = get_module_state(m);

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

    CREATE_TYPE(m, state->MemHive_Type, &MemHive_TypeSpec, NULL, 1);
    CREATE_TYPE(m, state->MemHiveProxy_Type, &MemHiveProxy_TypeSpec, NULL, 1);

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

    state->mutid_counter = 1;
    state->empty_bitmap_node = _map_node_bitmap_new(state, 0, 0);

    PyThreadState *tstate = PyThreadState_Get();
    assert(tstate != NULL);
    PyInterpreterState *interp = PyThreadState_GetInterpreter(tstate);
    assert(interp != NULL);
    state->interpreter_id = PyInterpreterState_GetID(interp);

    return 0;
}


PyMODINIT_FUNC
PyInit__memhive(void)
{
    return PyModuleDef_Init(&memhive_module);
}
