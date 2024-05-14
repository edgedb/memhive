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

    struct item *reuse;
    ssize_t reuse_num;

    uint8_t closed;
    uint8_t destroyed;
} MemQueue;

typedef enum {E_BROADCAST, E_REQUEST, E_PUSH} memqueue_event_t;

ssize_t
MemQueue_AddChannel(MemQueue *queue);

int
MemQueue_Broadcast(MemQueue *queue, PyObject *sender, PyObject *msg);

PyObject *
MemQueue_Push(MemQueue *queue, PyObject *sender, PyObject *val);

PyObject *
MemQueue_Request(MemQueue *queue, ssize_t channel, PyObject *sender, PyObject *val);

int
MemQueue_Listen(MemQueue *queue, module_state *state,
                ssize_t channel,
                memqueue_event_t *event, PyObject **sender, PyObject **val);

int
MemQueue_Init(MemQueue *queue);

int
MemQueue_Close(MemQueue *queue);

void
MemQueue_Destroy(MemQueue *queue);


#endif
