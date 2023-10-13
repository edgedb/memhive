#ifndef MEMHIVE_MODULE_H
#define MEMHIVE_MODULE_H


struct ProxyDescriptor;


typedef struct {
    int64_t interpreter_id;

    PyObject *empty_bitmap_node;
    uint64_t mutid_counter;

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
    PyTypeObject *MemHiveProxy_Type;

    struct ProxyDescriptor *proxy_desc_template;
} module_state;

#include "module.h"
#endif
