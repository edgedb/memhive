#ifndef MEMHIVE_MH_H
#define MEMHIVE_MH_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"


#define DistantPyObject PyObject

#define _ShareableObject(pref)          \
    PyObject_HEAD                       \
                                        \
    /* protected section */             \
    pthread_mutex_t pref##_mut;         \
    int8_t pref##_was_saved;            \
    int64_t pref##_ext_refs;



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
