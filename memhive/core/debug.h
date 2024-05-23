#ifndef MEMHIVE_DEBUG_MODULE_H
#define MEMHIVE_DEBUG_MODULE_H


#include "Python.h"


#if !defined(NDEBUG)
#define DEBUG
#endif


#ifdef DEBUG
// The purpose for this typedef is to use it instead of "PyObject*" in
// places where we're deling with Python objects from external interpreters.
typedef struct {
    PyObject_HEAD
} RemoteObject;
#else
#define RemoteObject PyObject
#endif


#define USED_IN_DEBUG(x) if(0) {(void)(x);}

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
