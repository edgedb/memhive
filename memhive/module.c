#include "memhive.h"
#include "map.h"


typedef struct {
    PyTypeObject *MemHive_Type;
    PyTypeObject *MemHiveProxy_Type;
} module_state;


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
    return 0;
}


static int
module_traverse(PyObject *mod, visitproc visit, void *arg)
{
    module_state *state = get_module_state(mod);
    Py_VISIT(state->MemHive_Type);
    Py_VISIT(state->MemHiveProxy_Type);
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

#define CREATE_TYPE(mod, tp, spec, base)                                \
    do {                                                                \
        tp = (PyTypeObject *)PyType_FromMetaclass(                      \
            NULL, mod, spec, (PyObject *)base);                         \
        if (tp == NULL || PyType_Ready(tp) < 0) {                       \
            return -1;                                                  \
        }                                                               \
        if (PyModule_AddType(mod, tp) < 0) {                            \
            return -1;                                                  \
        }                                                               \
    } while (0)

    CREATE_TYPE(m, state->MemHive_Type, &MemHive_TypeSpec, NULL);
    CREATE_TYPE(m, state->MemHiveProxy_Type, &MemHiveProxy_TypeSpec, NULL);

    // if (
    //     (PyType_Ready(&MemHive_Type) < 0) ||
    //     (PyType_Ready(&MemHiveProxy_Type) < 0)
    // )
    // {
    //     return 0;
    // }

    // if ((PyType_Ready(&_Map_Type) < 0) ||
    //     (PyType_Ready(&_MapMutation_Type) < 0) ||
    //     (PyType_Ready(&_Map_ArrayNode_Type) < 0) ||
    //     (PyType_Ready(&_Map_BitmapNode_Type) < 0) ||
    //     (PyType_Ready(&_Map_CollisionNode_Type) < 0) ||
    //     (PyType_Ready(&_MapKeys_Type) < 0) ||
    //     (PyType_Ready(&_MapValues_Type) < 0) ||
    //     (PyType_Ready(&_MapItems_Type) < 0) ||
    //     (PyType_Ready(&_MapKeysIter_Type) < 0) ||
    //     (PyType_Ready(&_MapValuesIter_Type) < 0) ||
    //     (PyType_Ready(&_MapItemsIter_Type) < 0))
    // {
    //     return 0;
    // }

    // Py_INCREF(&_Map_Type);
    // if (PyModule_AddObject(m, "Map", (PyObject *)&_Map_Type) < 0) {
    //     Py_DECREF(&_Map_Type);
    //     return NULL;
    // }

    return 0;
}


PyMODINIT_FUNC
PyInit__memhive(void)
{
    return PyModuleDef_Init(&memhive_module);
}
