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


// We want to be able to track if an object was created in our interpreter
// or in another one. One way to do it is to memorize all object IDs (their
// addresses) that ever pass through our API boundary or are created by us.
// We only need to keep track of addresses for this reason. Every
// subinterpreter has its separate memory allocator, so addresses can't be
// reused between them.
#define TRACK(state, o)                                                        \
    do {                                                                       \
        assert(o != NULL);                                                     \
        if (IS_TRACKING(state) && IS_TRACKABLE(state, o)) {                    \
            assert(o != NULL);                                                 \
            PyObject *_id = PyLong_FromVoidPtr(o);                             \
            if (_id == NULL) abort();                                          \
            if (PySet_Add(state->debug_objects_ids, _id)) abort();             \
            Py_DecRef(_id);                                                    \
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
