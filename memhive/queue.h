#ifndef MEMHIVE_QUEUE_H
#define MEMHIVE_QUEUE_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"

typedef struct {
    PyObject_HEAD

    uint64_t length;
    pthread_mutex_t mut;
    pthread_cond_t cond;

    struct item *first;
    struct item *last;

    uint8_t closed;
} MemQueue;

extern PyType_Spec MemQueue_TypeSpec;

PyObject *
MemQueue_Put(MemQueue *queue, PyObject *borrowed_val);

PyObject *
MemQueue_GetAndProxy(MemQueue *queue);

#endif
