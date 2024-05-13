#ifndef MEMHIVE_PROXY_H
#define MEMHIVE_PROXY_H

// We need a way to indicate that a Python object can be proxided
// from another sub-interpreter, and we need to be able to check that
// as fast as possible. Given that not too many people use Stackless
// nowadays it makes sense to just reuse the bit reserved for Stackless
// here. This will likely have to be fixed somehow later, I'm not
// too happy with this hack.
#define MEMHIVE_TPFLAGS_PROXYABLE    (1UL << 15)


// Safe to use on pointers from other subinterpreters as they only
// depend on the consistent structs layouts and constants being the
// same everywhere, which they are.
#define MEMHIVE_IS_PROXYABLE(op) \
    (PyType_FastSubclass(Py_TYPE(op), MEMHIVE_TPFLAGS_PROXYABLE))


typedef PyObject * (*module_unaryfunc)(module_state *, PyObject *);


typedef struct ProxyDescriptor {
    module_unaryfunc copy_from_main_to_sub;
    module_unaryfunc copy_from_sub_to_main;
} ProxyDescriptor;


typedef struct {
    PyObject_HEAD
    ProxyDescriptor* proxy_desc;
} ProxyableObject;


#endif

