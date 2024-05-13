#ifndef MEMHIVE_QUEUE_H
#define MEMHIVE_QUEUE_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"

#include "module.h"

typedef struct {
    pthread_mutex_t mut;
    pthread_cond_t cond;
    struct queue *queues;
    ssize_t nqueues;
    uint8_t closed;
    uint8_t destroyed;
} MemQueue;

ssize_t
MemQueue_AddChannel(MemQueue *queue);

PyObject *
MemQueue_Push(MemQueue *queue, PyObject *sender, PyObject *val);

int
MemQueue_Get(MemQueue *queue, module_state *state, PyObject **sender, PyObject **val);

int
MemQueue_Init(MemQueue *queue);

int
MemQueue_Close(MemQueue *queue);

int
MemQueue_Destroy(MemQueue *queue);


#endif
