#ifndef MEMHIVE_QUEUE_H
#define MEMHIVE_QUEUE_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"

#include "module.h"

typedef struct {
    uint64_t length;
    pthread_mutex_t mut;
    pthread_cond_t cond;

    struct item *first;
    struct item *last;

    uint8_t closed;
} MemQueue;

PyObject *
MemQueue_Put(MemQueue *queue, PyObject *sender, PyObject *val);

int
MemQueue_Get(MemQueue *queue, module_state *state, PyObject **sender, PyObject **val);

int
MemQueue_Init(MemQueue *queue);

int
MemQueue_Close(MemQueue *queue);

int
MemQueue_Destroy(MemQueue *queue);


#endif
