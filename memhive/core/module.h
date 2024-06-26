#ifndef MEMHIVE_MODULE_H
#define MEMHIVE_MODULE_H

#include "Python.h"
#include <stdint.h>
#include "debug.h"


struct ProxyDescriptor;


typedef struct {
    // The `MemHiveSub` instance that manages this module.
    // NULL for the main subinterpreter's `MemHive` -- we don't need it.
    PyObject *sub;
    int64_t interpreter_id;

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

    PyTypeObject *MemQueueRequestType;
    PyTypeObject *MemQueueResponseType;
    PyTypeObject *MemQueueBroadcastType;

    PyObject *exc_frames_cache;
    PyObject *exc_types_cache;
    PyObject *exc_empty_dict;

    PyObject *str_START;
    PyObject *str_ERROR;
    PyObject *str_CLOSE;

    struct ProxyDescriptor *proxy_desc_template;

#ifdef DEBUG
    int debug_tracking;
    PyObject *debug_objects_ids;
#endif
} module_state;

module_state * MemHive_GetModuleState(PyObject *mod);
module_state * MemHive_GetModuleStateByType(PyTypeObject *cls);
module_state * MemHive_GetModuleStateByPythonType(PyTypeObject *cls);
module_state * MemHive_GetModuleStateByObj(PyObject *obj);


#endif
