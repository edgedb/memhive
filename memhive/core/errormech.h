#ifndef MEMHIVE_ERRORMECH_H
#define MEMHIVE_ERRORMECH_H

#include "Python.h"

PyObject * MemHive_DumpError(PyObject *err);
PyObject * MemHive_RestoreError(PyObject *ers);

#endif
