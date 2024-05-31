#ifndef MEMHIVE_UTILS_H
#define MEMHIVE_UTILS_H

#include "module.h"
#include "debug.h"

// A type alias for PyObject* pointers to objects owned by a different
// sub-interpreter.

PyObject * MemHive_CopyObject(module_state *, RemoteObject *);

#endif
