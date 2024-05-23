#ifndef MEMHIVE_ERRORMECH_H
#define MEMHIVE_ERRORMECH_H

#include "Python.h"
#include "module.h"
#include "debug.h"

PyObject * MemHive_DumpError(PyObject *err);
PyObject * MemHive_RestoreError(module_state *state, RemoteObject *ers);

#endif
