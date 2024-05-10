#ifndef MEMHIVE_DEBUG_MODULE_H
#define MEMHIVE_DEBUG_MODULE_H

#if !defined(NDEBUG)
#define DEBUG
#endif

#define USED_IN_DEBUG(x) if(0) {(void)(x);}

#ifdef DEBUG
#define PO(what, obj)                                                          \
    do {                                                                       \
        printf(what);                                                          \
        printf(" ");                                                           \
        if (obj != NULL) {                                                     \
            PyObject_Print((PyObject*)obj, stdout, 0);                         \
        } else {                                                               \
            printf("!NULL!");                                                  \
        }                                                                      \
        printf("\n");                                                          \
    } while(0)
#endif

#endif
