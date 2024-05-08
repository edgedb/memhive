#ifndef MEMHIVE_QUEUE_H
#define MEMHIVE_QUEUE_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"

#include "module.h"

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
MemQueue_Put(MemQueue *queue, PyObject *sender, PyObject *val);

int
MemQueue_Get(MemQueue *queue, module_state *state, PyObject **sender, PyObject **val);

MemQueue *
NewMemQueue(module_state *calling_state);

int
MemQueue_Close(MemQueue *queue);

#endif
