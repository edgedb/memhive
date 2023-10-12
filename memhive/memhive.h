#ifndef MEMHIVE_MH_H
#define MEMHIVE_MH_H

// We need a threadsafe malloc, so C11 it is.
#if __STDC_VERSION__ < 201112L
#error "This header file requires C11"
#endif

#include <stdint.h>
#include <pthread.h>

#include "Python.h"

#include "map.h"


// We need a way to indicate that a Python object can be proxided
// from another sub-interpreter, and we need to be able to check that
// as fast as possible. Given that not too many people use Stackless
// nowadays it makes sense to just reuse the bit reserved for Stackless
// here. This will likely have to be fixed somehow later, I'm not
// too happy with this hack.
#define MEMHIVE_TPFLAGS_PROXYABLE    (1UL << 15)


// Both of the following macros are safe to use on pointers from
// other subinterpreters as they only depent on the consistent structs
// layouts and constants being the same everywhere, which they are.
#define MEMHIVE_IS_PROXYABLE(op) \
    (PyType_FastSubclass(Py_TYPE(op), MEMHIVE_TPFLAGS_PROXYABLE))
#define MEMHIVE_IS_COPYABLE(op) \
    (PyUnicode_Check(op) || PyLong_Check(op) || PyBytes_Check(op) || \
     PyFloat_Check(op))

// We want to minimize
#define MEMHIVE_IS_VALID_KEY(op)    PyUnicode_Check(op)


// A type alias for PyObject* pointers to objects owned by a different
// sub-interpreter.
typedef PyObject DistantPyObject;

PyObject * MemHive_CopyObject(DistantPyObject *);


typedef struct {
    PyObject_HEAD

    pthread_rwlock_t index_rwlock;

    // We're going to use a Python dict for the index, which is
    // somewhat suboptimal given that we will be calling read
    // dict lookup APIs from other threads. This should be fine,
    // considering:
    //
    // * we will have a custom RWLock surrounding _ALL_ operations
    //   with the index.
    //
    // * we will make the possibility of Python needing to
    //   raise errors near 0.
    //
    // * hashes of the same strings are equal across subinterpreters
    //   (this is assumed everywhere, so if this assumption is broken
    //   in some future version of Python we'll need to workaround
    //   by using custom hash functions for all types we support.)
    PyObject *index;
} MemHive;

extern PyType_Spec MemHive_TypeSpec;


// MemHive objects API, every method is safe to call from
// subinterpeters.

Py_ssize_t MemHive_Len(MemHive *hive);

// Gets a value for the key, NULL on KeyError.
// The value will be safe to use in the calling subinterpreter.
PyObject * MemHive_Get(MemHive *hive, PyObject *key);



typedef struct {
    PyObject_HEAD

    DistantPyObject *hive;
} MemHiveProxy;

extern PyType_Spec MemHiveProxy_TypeSpec;


typedef struct {
    MapNode *empty_bitmap_node;
    uint64_t mutid_counter;

    PyTypeObject *MapType;
    PyTypeObject *MapMutationType;

    PyTypeObject *ArrayNodeType;
    PyTypeObject *BitmapNodeType;
    PyTypeObject *CollisionNodeType;

    PyTypeObject *MapItemsType;
    PyTypeObject *MapItemsIterType;
    PyTypeObject *MapValuesType;
    PyTypeObject *MapValuesIterType;
    PyTypeObject *MapKeysType;
    PyTypeObject *MapKeysIterType;

    PyTypeObject *MemHive_Type;
    PyTypeObject *MemHiveProxy_Type;
} module_state;

MapNode *
_map_node_bitmap_new(module_state *state, Py_ssize_t size, uint64_t mutid);


#endif
