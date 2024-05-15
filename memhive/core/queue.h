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
typedef enum {D_FROM_MAIN, D_FROM_SUB} memqueue_direction_t;

typedef struct {
    PyObject_HEAD
    PyObject *r_owner;
    memqueue_direction_t r_dir;
    memqueue_event_t r_kind;
    ssize_t r_channel;
    uint8_t r_used;
} MemQueueReplyCallback;

extern PyType_Spec MemQueueReplyCallback_TypeSpec;
extern PyStructSequence_Desc QueueMessage_Desc;

ssize_t
MemQueue_AddChannel(MemQueue *queue);

int
MemQueue_Broadcast(MemQueue *queue, PyObject *sender, PyObject *msg);

int
MemQueue_Push(MemQueue *queue, ssize_t channel, PyObject *sender, PyObject *val);

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

PyObject *
MemQueueReplyCallback_New(module_state *state,
                          PyObject *owner, memqueue_direction_t dir,
                          ssize_t channel,
                          memqueue_event_t kind);

PyObject *
MemQueueMessage_New(module_state *state,
                    memqueue_event_t kind, PyObject *payload, PyObject *reply);


#endif
