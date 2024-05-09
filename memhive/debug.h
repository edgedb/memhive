#ifndef MEMHIVE_DEBUG_MODULE_H
#define MEMHIVE_DEBUG_MODULE_H

#include "Python.h"

#if !defined(NDEBUG)
#define DEBUG
#endif

#ifdef DEBUG
#define IS_GENERALLY_TRACKABLE(o)                                              \
    (!(                                                                        \
        o == Py_None || o == Py_True || o == Py_False || o == Py_Ellipsis      \
        || _Py_IsImmortal(o)                                                   \
    ))

#define TRACK(state, o)                                                        \
    do {                                                                       \
        int tracking = state->debug_tracking;                                  \
        if (tracking && IS_GENERALLY_TRACKABLE(o)) {                           \
            assert(o != NULL);                                                 \
            if (PySet_Add(state->debug_objects, o)) abort();                   \
            PyObject *id = PyLong_FromVoidPtr(o);                              \
            if (id == NULL) abort();                                           \
            if (PySet_Add(state->debug_objects_ids, id)) abort();              \
            Py_DecRef(id);                                                     \
        }                                                                      \
    } while (0);
#else
#define TRACK(state, o)
#endif

#endif
