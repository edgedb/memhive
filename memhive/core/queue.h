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
    ssize_t max_queues;

    struct item *reuse;
    ssize_t reuse_num;

    uint8_t closed;
    uint8_t destroyed;
} MemQueue;

typedef enum {
    E_HUB_BROADCAST,
    E_HUB_REQUEST,
    E_HUB_PUSH,
    E_HEALTH_ERROR,
    E_HEALTH_START,
    E_HEALTH_CLOSE,
} memqueue_event_t;

typedef enum {D_FROM_MAIN, D_FROM_SUB} memqueue_direction_t;

typedef struct {
    PyObject_HEAD
    PyObject *r_owner;
    PyObject *r_arg;
    memqueue_direction_t r_dir;
    ssize_t r_channel;
    uint64_t r_id;
    uint8_t r_used;
} MemQueueRequest;

typedef struct {
    PyObject_HEAD
    PyObject *x_data;
    PyObject *x_error;
    uint64_t x_id;
} MemQueueResponse;

typedef struct {
    PyObject_HEAD
    PyObject *b_arg;
} MemQueueBroadcast;

extern PyType_Spec MemQueueResponse_TypeSpec;
extern PyType_Spec MemQueueRequest_TypeSpec;
extern PyType_Spec MemQueueBroadcast_TypeSpec;

ssize_t
MemQueue_AddChannel(MemQueue *queue, module_state *state);

int
MemQueue_Put(MemQueue *queue,
             module_state *state,
             memqueue_event_t kind,
             ssize_t channel,
             PyObject *sender,
             uint64_t id,
             PyObject *val);

int
MemQueue_HubBroadcast(MemQueue *queue,  module_state *state,
                      PyObject *sender, PyObject *msg);

int
MemQueue_HubPush(MemQueue *queue, module_state *state,
                 ssize_t channel, PyObject *sender, uint64_t id, PyObject *val);

int
MemQueue_HubRequest(MemQueue *queue, module_state *state,
                    ssize_t channel, PyObject *sender, uint64_t id, PyObject *val);

int
MemQueue_Listen(MemQueue *queue, module_state *state,
                ssize_t channel,
                memqueue_event_t *event, RemoteObject **sender,
                uint64_t *id, RemoteObject **val);

int
MemQueue_Init(MemQueue *queue, ssize_t max_side_channels);

int
MemQueue_Close(MemQueue *queue, module_state *state);

void
MemQueue_Destroy(MemQueue *queue);

PyObject *
MemQueueRequest_New(module_state *state,
                    PyObject *owner,
                    PyObject *arg,
                    memqueue_direction_t dir,
                    ssize_t channel,
                    uint64_t id);

PyObject *
MemQueueResponse_New(module_state *state,
                     PyObject *data,
                     PyObject *error,
                     uint64_t id);

PyObject *
MemQueueBroadcast_New(module_state *state, PyObject *arg);

#endif
