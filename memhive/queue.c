#include "queue.h"
#include "utils.h"

struct item {
    // A borrowed ref; the lifetime will have to be managed
    // outside. `item` objects should be free-able from other
    // threads as they "pop" them from the queue.
    PyObject *borrowed_val;
    struct item *next;
};

static int
memqueue_tp_init(MemQueue *o, PyObject *args, PyObject *kwds)
{
    if (pthread_mutex_init(&o->mut, NULL)) {
        Py_FatalError("Failed to initialize a mutex");
    }

    if (pthread_cond_init(&o->cond, NULL)) {
        Py_FatalError("Failed to initialize a condition");
    }

    o->length = 0;
    o->first = NULL;
    o->last = NULL;
    o->closed = 0;
    return 0;
}

PyObject *
MemQueue_Put(MemQueue *queue, PyObject *borrowed_val)
{
    // OK to read `queue->closed` without the lock as only
    // the owner thread can set it.
    if (queue->closed == 1) {
        PyErr_SetString(PyExc_ValueError,
                        "can't put, the queue is closed");
        return NULL;
    }

    struct item *i;
    i = malloc(sizeof *i);
    if (i == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    pthread_mutex_lock(&queue->mut);
    Py_END_ALLOW_THREADS

    i->borrowed_val = borrowed_val;
    i->next = NULL;
    if (queue->last == NULL) {
        queue->last = i;
        queue->first = i;
    } else {
        queue->last->next = i;
        queue->last = i;
    }

    if (queue->length == 0) {
        pthread_cond_broadcast(&queue->cond);
    }

    queue->length++;
    pthread_mutex_unlock(&queue->mut);

    Py_RETURN_NONE;
}

static PyObject *
memqueue_get(MemQueue *queue, module_state *state)
{
    Py_BEGIN_ALLOW_THREADS
    pthread_mutex_lock(&queue->mut);
    Py_END_ALLOW_THREADS

    while (queue->first == NULL && queue->closed == 0) {
        Py_BEGIN_ALLOW_THREADS
        pthread_cond_wait(&queue->cond, &queue->mut);
        Py_END_ALLOW_THREADS
        if (PyErr_CheckSignals()) {
            queue->closed = 1;
            pthread_cond_broadcast(&queue->cond);
            pthread_mutex_unlock(&queue->mut);
            return NULL;
        }
    }

    if (queue->closed == 1) {
        pthread_mutex_unlock(&queue->mut);
        PyErr_SetString(state->ClosedQueueError,
                        "can't get, the queue is closed");
        return NULL;
    }

    struct item *prev_first = queue->first;
    PyObject *borrowed_val = prev_first->borrowed_val;
    queue->first = prev_first->next;
    queue->length--;

    if (queue->first == NULL) {
        queue->last = NULL;
        queue->length = 0;
    }

    free(prev_first);
    pthread_mutex_unlock(&queue->mut);

    return borrowed_val;
}

PyObject *
MemQueue_GetAndProxy(MemQueue *queue, module_state *state)
{
    PyObject *borrowed = memqueue_get(queue, state);
    if (borrowed == NULL) {
        return NULL;
    }
    return MemHive_CopyObject(state, borrowed);
}

int
MemQueue_Close(MemQueue *queue)
{
    // OK to read `queue->closed` without the lock as only
    // the owner thread can set it.
    if (queue->closed == 1) {
        return 0;
    }
    queue->closed = 1;
    Py_BEGIN_ALLOW_THREADS
    pthread_mutex_lock(&queue->mut);
    Py_END_ALLOW_THREADS
    if (queue->length == 0) {
        pthread_cond_broadcast(&queue->cond);
    }
    pthread_mutex_unlock(&queue->mut);

    return 0;
}

MemQueue *
NewMemQueue(module_state *calling_state)
{
    MemQueue *o;
    o = PyObject_GC_New(MemQueue, calling_state->MemQueue_Type);
    if (o == NULL) {
        return NULL;
    }
    if (memqueue_tp_init(o, NULL, NULL)) {
        return NULL;
    }
    return o;
}

PyType_Slot MemQueue_TypeSlots[] = {
    {Py_tp_init, (initproc)memqueue_tp_init},
    {0, NULL},
};

PyType_Spec MemQueue_TypeSpec = {
    .name = "memhive._MemQueue",
    .basicsize = sizeof(MemQueue),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots = MemQueue_TypeSlots,
};
