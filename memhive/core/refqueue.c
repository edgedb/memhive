#include "refqueue.h"
#include "track.h"
#include "module.h"

#define MAX_REUSE 100

struct item {
    RemoteObject *obj;
    struct item *next;
};

RefQueue *
MemHive_RefQueue_New(void)
{
    RefQueue *q = PyMem_RawMalloc(sizeof(RefQueue));
    if (q == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    q->first_inc = NULL;
    q->last_inc = NULL;

    q->first_dec = NULL;
    q->last_dec = NULL;

    q->reuse = NULL;
    q->reuse_num = 0;

    q->closed = 0;

    pthread_mutex_init(&q->mut, NULL);

    return q;
}

static int
push_incdec(RefQueue *q, RemoteObject *obj, int is_inc)
{
    if (pthread_mutex_trylock(&q->mut)) {
        Py_BEGIN_ALLOW_THREADS
        pthread_mutex_lock(&q->mut);
        Py_END_ALLOW_THREADS
    }

    if (q->closed) {
        pthread_mutex_unlock(&q->mut);
        PyErr_SetString(PyExc_ValueError,
                        "can't put, the refqueue is closed");
        return -1;
    }

    struct item *cnt;
    if (q->reuse != NULL) {
        cnt = q->reuse;
        q->reuse = cnt->next;
        q->reuse_num--;
    } else {
        cnt = PyMem_RawMalloc(sizeof *cnt);
        if (cnt == NULL) {
            pthread_mutex_unlock(&q->mut);
            PyErr_NoMemory();
            return -1;
        }
    }

    cnt->next = NULL;
    cnt->obj = obj;

    #define PUSH(what, cnt)                 \
        if (q->last_##what == NULL) {       \
            q->last_##what = cnt;           \
            q->first_##what = cnt;          \
        } else {                            \
            q->last_##what->next = cnt;     \
            q->last_##what = cnt;           \
        }

    if (is_inc) {
        PUSH(inc, cnt)
    } else {
        PUSH(dec, cnt)
    }

    pthread_mutex_unlock(&q->mut);
    return 0;
}

int
MemHive_RefQueue_Inc(RefQueue *queue, RemoteObject *obj)
{
    return push_incdec(queue, obj, 1);
}

int
MemHive_RefQueue_Dec(RefQueue *queue, RemoteObject *obj)
{
    return push_incdec(queue, obj, 0);
}

void
MemHive_RefQueue_Run(RefQueue *q, module_state *state)
{
    struct item* incs;
    struct item* decs;

   if (pthread_mutex_trylock(&q->mut)) {
        Py_BEGIN_ALLOW_THREADS
        pthread_mutex_lock(&q->mut);
        Py_END_ALLOW_THREADS
    }

    incs = q->first_inc;
    decs = q->first_dec;

    q->first_inc = q->last_inc = NULL;
    q->first_dec = q->last_dec = NULL;

    pthread_mutex_unlock(&q->mut);

    struct item* to_reuse = NULL;

    while (incs != NULL) {
        assert(IS_LOCALLY_TRACKED(state, incs->obj));
        Py_INCREF(incs->obj);
        incs->obj = NULL;

        struct item* next = incs->next;

        incs->next = to_reuse;
        to_reuse = incs;

        incs = next;
    }

    while (decs != NULL) {
        assert(IS_LOCALLY_TRACKED(state, decs->obj));
        Py_DECREF(decs->obj);
        decs->obj = NULL;

        struct item* next = decs->next;

        decs->next = to_reuse;
        to_reuse = decs;

        decs = next;
    }

    if (to_reuse != NULL) {
        if (pthread_mutex_trylock(&q->mut)) {
            Py_BEGIN_ALLOW_THREADS
            pthread_mutex_lock(&q->mut);
            Py_END_ALLOW_THREADS
        }
        while (to_reuse != NULL && q->reuse_num < MAX_REUSE) {
            struct item* next = to_reuse;
            to_reuse = to_reuse->next;

            next->next = q->reuse;
            next->obj = NULL;
            q->reuse = next;

            q->reuse_num++;
        }
        pthread_mutex_unlock(&q->mut);
    }

    while (to_reuse != NULL) {
        struct item* next = to_reuse;
        to_reuse = to_reuse->next;
        PyMem_RawFree(next);
    }
}

int
MemHive_RefQueue_Destroy(RefQueue *q)
{
    if (q->closed) {
        return 0;
    }

    q->closed = 1;

    if (q->first_inc != NULL || q->first_dec != NULL) {
        PyErr_SetString(PyExc_ValueError,
                        "destroying refqueue with objects in it");
        return -1;
    }

    assert(q->last_inc == NULL);
    assert(q->last_dec == NULL);

    while (q->reuse != NULL) {
        struct item* next = q->reuse->next;
        PyMem_RawFree(q->reuse);
        q->reuse = next;
        q->reuse_num--;
    }

    assert(q->reuse_num == 0);

    return 0;
}
