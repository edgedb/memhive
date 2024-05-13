#include "queue.h"
#include "utils.h"

struct item {
    // A borrowed ref; the lifetime will have to be managed
    // outside. `item` objects should be free-able from other
    // threads as they "pop" them from the queue.
    PyObject *val;
    PyObject *sender;
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
queue_lock(MemQueue *queue)
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
        PyErr_SetString(PyExc_ValueError, "the queue is closed");
        return -1;
    }
    return 0;
}

static int
queue_put(MemQueue *queue, struct queue *q,
          PyObject *sender, memqueue_event_t kind, PyObject *val)
{
    // The lock must be held for this operation

    struct item *i;
    i = malloc(sizeof *i);
    if (i == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    Py_INCREF(val);
    i->val = val;
    i->kind = kind;

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
MemQueue_Init(MemQueue *o)
{
    if (pthread_mutex_init(&o->mut, NULL)) {
        Py_FatalError("Failed to initialize a mutex");
    }

    if (pthread_cond_init(&o->cond, NULL)) {
        Py_FatalError("Failed to initialize a condition");
    }

    o->queues = PyMem_RawMalloc(sizeof (struct queue));
    if (o->queues == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    o->queues[0].first = NULL;
    o->queues[0].last = NULL;
    o->queues[0].length = 0;

    o->nqueues = 1;

    o->closed = 0;
    o->destroyed = 0;
    return 0;
}

ssize_t
MemQueue_AddChannel(MemQueue *queue)
{
    ssize_t channel;

    if (queue_lock(queue)) {
        return -1;
    }

    struct queue *nq = PyMem_RawMalloc(
        (size_t)(queue->nqueues + 1) * (sizeof (struct queue))
    );
    if (nq == NULL) {
        PyErr_NoMemory();
        goto err;
    }

    memcpy(nq, queue->queues, (size_t)queue->nqueues * (sizeof (struct queue)));

    channel = queue->nqueues;
    nq[channel].first = NULL;
    nq[channel].last = NULL;
    nq[channel].length = 0;

    queue->nqueues++;

    PyMem_RawFree(queue->queues);
    queue->queues = nq;

    queue_unlock(queue);
    return channel;

err:
    queue_unlock(queue);
    return -1;
}

int
MemQueue_Broadcast(MemQueue *queue, PyObject *sender, PyObject *msg)
{
    if (queue_lock(queue)) {
        return -1;
    }

    for (ssize_t i = 1; i < queue->nqueues; i++) {
        if (queue_put(queue, &queue->queues[i], sender, E_BROADCAST, msg)) {
            queue_unlock(queue);
            return -1;
        }
    }

    queue_unlock(queue);
    return 0;
}

PyObject *
MemQueue_Request(MemQueue *queue, ssize_t channel, PyObject *sender, PyObject *val)
{
    if (queue_lock(queue)) {
        return NULL;
    }

    if (queue_put(queue, &queue->queues[channel], sender, E_REQUEST, val)) {
        queue_unlock(queue);
        return NULL;
    }

    queue_unlock(queue);

    Py_RETURN_NONE;
}

PyObject *
MemQueue_Push(MemQueue *queue, PyObject *sender, PyObject *val)
{
    if (queue_lock(queue)) {
        return NULL;
    }

    if (queue_put(queue, &queue->queues[0], sender, E_PUSH, val)) {
        queue_unlock(queue);
        return NULL;
    }

    queue_unlock(queue);

    Py_RETURN_NONE;
}

int
MemQueue_Listen(MemQueue *queue, module_state *state,
                ssize_t channel,
                memqueue_event_t *event, PyObject **sender, PyObject **val)
{
    if (queue_lock(queue)) {
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
            if (queue->closed == 1) {
                PyErr_SetString(state->ClosedQueueError,
                                "can't get, the queue is closed");
                return -1;
            }
            continue;
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
    *val = prev_first->val;
    *sender = prev_first->sender;

    q->first = prev_first->next;
    q->length--;

    if (q->first == NULL) {
        q->last = NULL;
        q->length = 0;
    }

    free(prev_first);
    queue_unlock(queue);

    return 0;
}

int
MemQueue_Close(MemQueue *queue)
{
    // OK to read `queue->closed` without the lock as only
    // the owner thread can set it.
    if (queue->closed == 1) {
        return 0;
    }
    if (queue_lock(queue)) {
        return -1;
    }
    queue->closed = 1;
    pthread_cond_broadcast(&queue->cond);
    queue_unlock(queue);
    return 0;
}

int
MemQueue_Destroy(MemQueue *queue)
{
    assert(queue->closed);
    if (queue->destroyed) {
        return 0;
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

    return 0;
}
