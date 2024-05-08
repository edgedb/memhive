#ifndef MEMHIVE_MODULE_H
#define MEMHIVE_MODULE_H

#include "Python.h"
#include <stdint.h>


struct ProxyDescriptor;


typedef struct {
    int64_t interpreter_id;

    PyObject *empty_bitmap_node;
    uint64_t mutid_counter;

    PyObject *ClosedQueueError;

    PyTypeObject *MapType;
    PyTypeObject *MapMutationType;

    PyTypeObject *ArrayNodeType;
    PyTypeObject *BitmapNodeType;
    PyTypeObject *CollisionNodeType;

    PyTypeObject *MapItemsType;
    PyTypeObject *MapItemsIterType;
    PyTypeObject *MapValuesType;
    PyTypeObject *MapValuesIterType;
    PyTypeObject *MapKeysType;
    PyTypeObject *MapKeysIterType;

    PyTypeObject *MemHive_Type;
    PyTypeObject *MemHiveSub_Type;
    PyTypeObject *MemQueue_Type;

    struct ProxyDescriptor *proxy_desc_template;
} module_state;

module_state * MemHive_GetModuleState(PyObject *mod);
module_state * MemHive_GetModuleStateByType(PyTypeObject *cls);
module_state * MemHive_GetModuleStateByPythonType(PyTypeObject *cls);
module_state * MemHive_GetModuleStateByObj(PyObject *obj);


#endif
