#ifndef MEMHIVE_DEBUG_MODULE_H
#define MEMHIVE_DEBUG_MODULE_H

#include "Python.h"

#if !defined(NDEBUG)
#define DEBUG
#endif

#ifdef DEBUG
#define IS_TRACKING(state)                                                     \
    (state->debug_tracking != 0)

#define IS_TRACKABLE(state, o)                                                 \
    (!(                                                                        \
        o == Py_None || o == Py_True || o == Py_False || o == Py_Ellipsis      \
        || _Py_IsImmortal(o)                                                   \
    ))

#define TRACK(state, o)                                                        \
    do {                                                                       \
        assert(o != NULL);                                                     \
        if (IS_TRACKING(state) && IS_TRACKABLE(state, o)) {                    \
            assert(o != NULL);                                                 \
            PyObject *id = PyLong_FromVoidPtr(o);                              \
            if (id == NULL) abort();                                           \
            if (PySet_Contains(state->debug_objects_ids, id) == 0) {           \
                if (PyList_Append(state->debug_objects, o)) abort();           \
                if (PySet_Add(state->debug_objects_ids, id)) abort();          \
            }                                                                  \
            Py_DecRef(id);                                                     \
        }                                                                      \
    } while (0);
#else
#define TRACK(state, o)
#endif

#ifdef DEBUG
#define PO(what, obj)                                                          \
    do {                                                                       \
        printf(what);                                                          \
        printf(" ");                                                           \
        if (obj != NULL) {                                                     \
            PyObject_Print((PyObject*)obj, stdout, 0);                         \
        } else {                                                               \
            printf("!NULL!");                                                  \
        }                                                                      \
        printf("\n");                                                          \
    } while(0)
#endif

#endif
