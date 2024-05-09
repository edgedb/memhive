#ifndef MEMHIVE_DEBUG_MODULE_H
#define MEMHIVE_DEBUG_MODULE_H

#include "Python.h"

#if !defined(NDEBUG)
#define DEBUG
#endif

#ifdef DEBUG
#define IS_GENERALLY_TRACKABLE(o)                                           \
    (!(                                                                     \
        o == Py_None || o == Py_True || o == Py_False || o == Py_Ellipsis   \
        || _Py_IsImmortal(o)                                                \
    ))
#endif

#endif
