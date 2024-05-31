#include "memhive.h"
#include "queue.h"
#include "utils.h"
#include "track.h"

#define MAX_REUSE 100
#define QUEUE_RESPONSE_TYPENAME "memhive.core.QueueResponse"

struct item {
    PyObject *val;
    PyObject *sender;
    uint64_t id;
    struct item *next;
    memqueue_event_t kind;
};

struct queue {
    struct item *first;
    struct item *last;
    ssize_t length;
};

static void
queue_unlock(MemQueue *queue)
{
    if (pthread_mutex_unlock(&((queue)->mut))) {
        Py_FatalError("can't release the queue lock");
    }
}

static int
queue_lock(MemQueue *queue, module_state *state)
{
    if ((queue)->destroyed) {
        Py_FatalError("queue has been destroyed");
    }
    if (pthread_mutex_trylock(&((queue)->mut))) {
        Py_BEGIN_ALLOW_THREADS
        if (pthread_mutex_lock(&((queue)->mut))) {
            Py_FatalError("can't acquire the queue lock");
        }
        Py_END_ALLOW_THREADS
    }
    if ((queue)->closed == 1) {
        queue_unlock(queue);
        PyErr_SetString(state->ClosedQueueError,
                        "can't acquire lock, the queue is closed");
        return -1;
    }
    return 0;
}

static int
queue_put(MemQueue *queue, struct queue *q,
          PyObject *sender, memqueue_event_t kind, uint64_t id, PyObject *val)
{
    // The lock must be held for this operation

    struct item *i;

    if (queue->reuse_num > 0) {
        i = queue->reuse;
        queue->reuse = i->next;
        queue->reuse_num--;
    } else {
        i = PyMem_RawMalloc(sizeof *i);
        if (i == NULL) {
            PyErr_NoMemory();
            return -1;
        }
    }

    Py_XINCREF(val);
    i->val = val;
    i->kind = kind;
    i->id = id;

    i->sender = sender;     // borrow; we don't care, the lifetime of sender is
                            // greater than of this queue

    i->next = NULL;
    if (q->last == NULL) {
        q->last = i;
        q->first = i;
    } else {
        q->last->next = i;
        q->last = i;
    }

    if (q->length == 0) {
        pthread_cond_broadcast(&queue->cond);
    }

    q->length++;

    return 0;
}

int
MemQueue_Init(MemQueue *o, ssize_t max_side_channels)
{
    if (pthread_mutex_init(&o->mut, NULL)) {
        Py_FatalError("Failed to initialize a mutex");
    }

    if (pthread_cond_init(&o->cond, NULL)) {
        Py_FatalError("Failed to initialize a condition");
    }

    o->queues = PyMem_RawMalloc(
        ((uint64_t)max_side_channels + 1) * (sizeof (struct queue))
    );
    if (o->queues == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    o->queues[0].first = NULL;
    o->queues[0].last = NULL;
    o->queues[0].length = 0;

    o->nqueues = 1;
    o->max_queues = max_side_channels;

    o->reuse = NULL;
    o->reuse_num = 0;

    o->closed = 0;
    o->destroyed = 0;
    return 0;
}

ssize_t
MemQueue_AddChannel(MemQueue *queue, module_state *state)
{
    ssize_t channel;

    if (queue_lock(queue, state)) {
        return -1;
    }

    if ((queue->nqueues + 1) >= queue->max_queues) {
        queue_unlock(queue);
        PyErr_SetString(PyExc_RuntimeError,
                        "max number of worker queues reached");
        return -1;
    }

    struct queue *qq = queue->queues;
    channel = queue->nqueues;
    qq[channel].first = NULL;
    qq[channel].last = NULL;
    qq[channel].length = 0;

    queue->nqueues++;

    queue_unlock(queue);
    return channel;
}

int
MemQueue_HubBroadcast(MemQueue *queue,  module_state *state,
                      PyObject *sender, PyObject *msg)
{
    if (queue_lock(queue, state)) {
        return -1;
    }

    for (ssize_t i = 1; i < queue->nqueues; i++) {
        if (queue_put(queue, &queue->queues[i],
                      sender, E_HUB_BROADCAST, 0, msg))
        {
            queue_unlock(queue);
            return -1;
        }
    }

    queue_unlock(queue);
    return 0;
}

MEMHIVE_REMOTE(int)
MemQueue_HubRequest(MemQueue *queue, module_state *state,
                    ssize_t channel, PyObject *sender, uint64_t id, PyObject *val)
{
    assert(val != NULL);
    return MemQueue_Put(queue, state, E_HUB_REQUEST, channel, sender, id, val);
}

int
MemQueue_HubPush(MemQueue *queue, module_state *state,
                 ssize_t channel, PyObject *sender, uint64_t id, PyObject *val)
{
    return MemQueue_Put(queue, state, E_HUB_PUSH, channel, sender, id, val);
}

MEMHIVE_REMOTE(int)
MemQueue_Put(MemQueue *queue,
             module_state *state,
             memqueue_event_t kind,
             ssize_t channel,
             PyObject *sender,
             uint64_t id,
             PyObject *val)
{
    if (queue_lock(queue, state)) {
        return -1;
    }
    int ret = queue_put(queue, &queue->queues[channel], sender, kind, id, val);
    queue_unlock(queue);
    return ret;
}

MEMHIVE_REMOTE(int)
MemQueue_Listen(MemQueue *queue, module_state *state,
                ssize_t channel,
                memqueue_event_t *event, RemoteObject **sender,
                uint64_t *id, RemoteObject **val)
{
    if (queue_lock(queue, state)) {
        return -1;
    }

    struct queue *q_push = &queue->queues[0];
    struct queue *q_mine = NULL;
    if (channel != 0) {
        assert(channel >= 1 && channel < queue->nqueues);
        q_mine = &queue->queues[channel];
    }

    while (
        queue->closed == 0
        && q_push->first == NULL
        && (q_mine == NULL || q_mine->first == NULL)
    ) {
        Py_BEGIN_ALLOW_THREADS
        pthread_cond_wait(&queue->cond, &queue->mut);
        Py_END_ALLOW_THREADS
        if (PyErr_CheckSignals()) {
            queue_unlock(queue);
            return -1;
        }
    }

    if (queue->closed == 1) {
        queue_unlock(queue);
        PyErr_SetString(state->ClosedQueueError,
                        "can't get, the queue is closed");
        return -1;
    }

    struct queue *q = q_push;
    if (q_mine != NULL && q_mine->first != NULL) {
        q = q_mine;
    }
    assert(q->first != NULL);

    struct item *prev_first = q->first;
    *event = prev_first->kind;
    *val = (RemoteObject*)prev_first->val;
    *sender = (RemoteObject*)prev_first->sender;
    *id = prev_first->id;

    q->first = prev_first->next;
    q->length--;

    if (q->first == NULL) {
        q->last = NULL;
        q->length = 0;
    }

    if (queue->reuse_num < MAX_REUSE) {
        prev_first->sender = NULL;
        prev_first->val = NULL;
        prev_first->next = queue->reuse;
        queue->reuse = prev_first;
        queue->reuse_num++;
    } else {
        PyMem_RawFree(prev_first);
    }

    queue_unlock(queue);

    return 0;
}

int
MemQueue_Close(MemQueue *queue, module_state *state)
{
    if (queue->closed == 1) {
        return 0;
    }
    if (queue_lock(queue, state)) {
        return -1;
    }
    queue->closed = 1;
    pthread_cond_broadcast(&queue->cond);
    queue_unlock(queue);
    return 0;
}

void
MemQueue_Destroy(MemQueue *queue)
{
    if (!queue->closed) {
        Py_FatalError("destroying a queue before closing");
    }
    if (queue->destroyed) {
        return;
    }
    queue->destroyed = 1;
    if (pthread_mutex_trylock(&queue->mut)) {
        Py_FatalError("lock is held by something in MemQueue_Destroy");
    }
    pthread_mutex_unlock(&queue->mut);
    if (pthread_cond_destroy(&queue->cond)) {
        Py_FatalError(
            "clould not destroy the conditional var in MemQueue_Destroy");
    }
    if (pthread_mutex_destroy(&queue->mut)) {
        Py_FatalError("clould not destroy the lock in MemQueue_Destroy");
    }

    while(queue->reuse != NULL) {
        struct item *next = queue->reuse->next;
        PyMem_RawFree(queue->reuse);
        queue->reuse = next;
    }
    queue->reuse_num = 0;

    for (ssize_t i = 0; i < queue->nqueues; i++) {
        struct queue *q = &queue->queues[i];
        while (q->first != NULL) {
            struct item *next = q->first->next;
            PyMem_RawFree(q->first);
            q->first = next;
        }
    }
    PyMem_RawFree(queue->queues);
    queue->queues = NULL;
    queue->nqueues = 0;
}


////////////////////////////////////////////////////////////////////////////////


PyObject *
MemQueueRequest_New(module_state *state,
                    PyObject *owner,
                    PyObject *arg,
                    memqueue_direction_t dir,
                    ssize_t channel,
                    uint64_t id)
{
    MemQueueRequest *o = PyObject_GC_New(
        MemQueueRequest, state->MemQueueRequestType);
    if (o == NULL) {
        return NULL;
    }

    #ifdef DEBUG
    if (!IS_LOCALLY_TRACKED(state, owner)) {
        Py_FatalError("Queue request expects a local owner object");
    }
    if (!IS_LOCALLY_TRACKED(state, arg)) {
        Py_FatalError("Queue request expects a local arg object");
    }
    #endif

    o->r_owner = Py_NewRef(owner);
    o->r_arg = Py_NewRef(arg);

    o->r_dir = dir;
    o->r_used = 0;
    o->r_channel = channel;
    o->r_id = id;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}

static PyObject *
mq_req_tp_call(MemQueueRequest *o, PyObject *args, PyObject *kwargs)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject*)o);

    if (o->r_used) {
        PyErr_SetString(
            PyExc_ValueError,
            "Queue request object was used before");
        return NULL;
    }

    if (kwargs != NULL) {
        PyErr_SetString(
            PyExc_TypeError,
            "Queue request does not support keyword arguments");
        return NULL;
    }

    PyObject *ret = NULL;
    if (!PyArg_UnpackTuple(args, QUEUE_RESPONSE_TYPENAME, 1, 1, &ret)) {
        return NULL;
    }
    assert(ret != NULL);
    TRACK(state, ret);

    o->r_used = 1;

    if (o->r_dir == D_FROM_SUB) {
        MemHive *hive = (MemHive *)(((MemHiveSub *)o->r_owner)->hive);
        int r = MemQueue_HubPush(
            &hive->for_main, state, o->r_channel,
            o->r_owner, o->r_id, ret);
        if (r) {
            return NULL;
        }
        Py_RETURN_NONE;
    } else if (o->r_dir == D_FROM_MAIN) {
        MemHive *hive = (MemHive *)o->r_owner;
        int r = MemQueue_HubRequest(
            &hive->for_subs, state, o->r_channel,
            o->r_owner, o->r_id, ret);
        if (r) {
            return NULL;
        }
        Py_RETURN_NONE;
    } else {
        abort();
    }
}

static int
mq_req_tp_traverse(MemQueueRequest *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->r_owner);
    Py_VISIT(self->r_arg);
    return 0;
}

static int
mq_req_tp_clear(MemQueueRequest *self)
{
    Py_CLEAR(self->r_owner);
    Py_CLEAR(self->r_arg);
    return 0;
}

static void
mq_req_tp_dealloc(MemQueueRequest *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    (void)mq_req_tp_clear(self);
    Py_TYPE(self)->tp_free(self);
    Py_DecRef((PyObject*)tp);
}

PyType_Slot MemQueueRequestMembers_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)mq_req_tp_dealloc},
    {Py_tp_traverse, (traverseproc)mq_req_tp_traverse},
    {Py_tp_clear, (inquiry)mq_req_tp_clear},
    {Py_tp_call, mq_req_tp_call},
    {0, NULL},
};

PyType_Spec MemQueueRequest_TypeSpec = {
    .name = QUEUE_RESPONSE_TYPENAME,
    .basicsize = sizeof(MemQueueRequest),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = MemQueueRequestMembers_TypeSlots,
};


////////////////////////////////////////////////////////////////////////////////


PyObject *
MemQueueResponse_New(module_state *state,
                     PyObject *data,
                     PyObject *error,
                     uint64_t id)
{
    MemQueueResponse *o = PyObject_GC_New(
        MemQueueResponse, state->MemQueueResponseType);
    if (o == NULL) {
        return NULL;
    }

    o->x_data = Py_XNewRef(data);
    o->x_error = Py_XNewRef(error);
    o->x_id = id;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


static int
mq_resp_tp_traverse(MemQueueResponse *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->x_data);
    Py_VISIT(self->x_error);
    return 0;
}

static int
mq_resp_tp_clear(MemQueueResponse *self)
{
    Py_CLEAR(self->x_data);
    Py_CLEAR(self->x_error);
    return 0;
}

static void
mq_resp_tp_dealloc(MemQueueResponse *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    (void)mq_resp_tp_clear(self);
    Py_TYPE(self)->tp_free(self);
    Py_DecRef((PyObject*)tp);
}

PyType_Slot MemQueueResponseMembers_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)mq_resp_tp_dealloc},
    {Py_tp_traverse, (traverseproc)mq_resp_tp_traverse},
    {Py_tp_clear, (inquiry)mq_resp_tp_clear},
    {0, NULL},
};

PyType_Spec MemQueueResponse_TypeSpec = {
    .name = QUEUE_RESPONSE_TYPENAME,
    .basicsize = sizeof(MemQueueResponse),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = MemQueueResponseMembers_TypeSlots,
};


////////////////////////////////////////////////////////////////////////////////


PyObject *
MemQueueBroadcast_New(module_state *state, PyObject *arg)
{
    MemQueueBroadcast *o = PyObject_GC_New(
        MemQueueBroadcast, state->MemQueueBroadcastType);
    if (o == NULL) {
        return NULL;
    }

    o->b_arg = Py_NewRef(arg);

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


static int
mq_broad_tp_traverse(MemQueueBroadcast *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->b_arg);
    return 0;
}

static int
mq_broad_tp_clear(MemQueueBroadcast *self)
{
    Py_CLEAR(self->b_arg);
    return 0;
}

static void
mq_broad_tp_dealloc(MemQueueBroadcast *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    (void)mq_broad_tp_clear(self);
    Py_TYPE(self)->tp_free(self);
    Py_DecRef((PyObject*)tp);
}

PyType_Slot MemQueueBroadcastMembers_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)mq_broad_tp_dealloc},
    {Py_tp_traverse, (traverseproc)mq_broad_tp_traverse},
    {Py_tp_clear, (inquiry)mq_broad_tp_clear},
    {0, NULL},
};

PyType_Spec MemQueueBroadcast_TypeSpec = {
    .name = QUEUE_RESPONSE_TYPENAME,
    .basicsize = sizeof(MemQueueBroadcast),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = MemQueueBroadcastMembers_TypeSlots,
};
