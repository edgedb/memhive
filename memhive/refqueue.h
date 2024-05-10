#ifndef MEMHIVE_REFQUEUE_H
#define MEMHIVE_REFQUEUE_H

#include <stdint.h>
#include <pthread.h>

#include "Python.h"

#include "module.h"

typedef struct {
    pthread_mutex_t mut;

    struct item *first_inc;
    struct item *last_inc;

    struct item *first_dec;
    struct item *last_dec;

    struct item *reuse;
    uint32_t reuse_num;

    uint8_t closed;
} RefQueue;

RefQueue *MemHive_RefQueue_New(void);
int MemHive_RefQueue_Inc(RefQueue *queue, PyObject *obj);
int MemHive_RefQueue_Dec(RefQueue *queue, PyObject *obj);
int MemHive_RefQueue_Run(RefQueue *queue, module_state *state);
int MemHive_RefQueue_Destroy(RefQueue *queue);

#endif
