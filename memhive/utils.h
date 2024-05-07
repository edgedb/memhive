#ifndef MEMHIVE_UTILS_H
#define MEMHIVE_UTILS_H

#include "module.h"

// A type alias for PyObject* pointers to objects owned by a different
// sub-interpreter.
typedef PyObject DistantPyObject;

PyObject * MemHive_CopyObject(module_state *, DistantPyObject *);

#endif
