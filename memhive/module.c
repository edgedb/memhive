#include "memhive.h"


static struct PyModuleDef _module = {
    PyModuleDef_HEAD_INIT,      /* m_base */
    "_memhive",                 /* m_name */
    NULL,                       /* m_doc */
    -1,                         /* m_size */
    NULL,                       /* m_methods */
    NULL,                       /* m_slots */
    NULL,                       /* m_traverse */
    NULL,                       /* m_clear */
    NULL,                       /* m_free */
};


PyMODINIT_FUNC
PyInit__memhive(void)
{
    PyObject *m = PyModule_Create(&_module);

    if (
        (PyType_Ready(&MemHive_Type) < 0) ||
        (PyType_Ready(&MemHiveProxy_Type) < 0)
    )
    {
        return 0;
    }

    Py_INCREF(&MemHive_Type);
    if (PyModule_AddObject(m, "_MemHive", (PyObject *)&MemHive_Type) < 0) {
        Py_DECREF(&MemHive_Type);
        return NULL;
    }

    Py_INCREF(&MemHiveProxy_Type);
    if (PyModule_AddObject(m, "_MemHiveProxy", (PyObject *)&MemHiveProxy_Type) < 0) {
        Py_DECREF(&MemHiveProxy_Type);
        return NULL;
    }

    return m;
}
