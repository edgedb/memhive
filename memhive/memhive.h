#ifndef MEMHIVE_MH_H
#define MEMHIVE_MH_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"


// We need a way to indicate that a Python object can be proxided
// from another sub-interpreter, and we need to be able to check that
// as fast as possible. Given that not too many people use Stackless
// nowadays it makes sense to just use the bit reserved for Stackless
// here. This will likely have to be fixed somehow later, I'm not
// too happy with this hack.
#define MEMHIVE_TPFLAG_PROXYABLE    (1UL << 15)


// Both of the following macros are safe to use on pointers from
// other subinterpreters as they only depent on the consistent structs
// layouts and constants being the same everywhere, which they are.
#define MEMHIVE_IS_PROXYABLE(op) \
    (PyType_FastSubclass(Py_TYPE(op), MEMHIVE_TPFLAG_PROXYABLE))
#define MEMHIVE_IS_COPYABLE(op) \
    (PyUnicode_Check(op) || PyLong_Check(op) || PyBytes_Check(op) || \
     PyFloat_Check(op))
#define MEMHIVE_IS_VALID_KEY(op)    MEMHIVE_IS_COPYABLE(op)


// A type alias for PyObject* pointers to objects owned by a different
// sub-interpreter.
typedef PyObject DistantPyObject;

PyObject * MemHive_CopyObject(DistantPyObject *);


typedef struct {
    PyObject_HEAD

    pthread_rwlock_t index_rwlock;
    PyObject *index;
} MemHive;

PyTypeObject MemHive_Type;


typedef struct {
    PyObject_HEAD

    DistantPyObject *hive;
} MemHiveProxy;

PyTypeObject MemHiveProxy_Type;



#endif
