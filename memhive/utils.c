#include "memhive.h"
#include "debug.h"


PyObject *
MemHive_CopyObject(module_state *calling_state, DistantPyObject *o)
{
    assert(o != NULL);

    // This deserves an explanation: it's only safe to use immortal objects
    // created in the main subinterpreter (`interpreter_id == 0`).
    // Our condition here is the reverse of that, because we only can
    // use immortal objects in non-main interpreters! Under no circumstance
    // we should allow an immortal object from a subinterpreter to be used
    // in main, because that object will cease to exist when the subinterpreter
    // that created it is done.
    //
    // That said, in debug mode we don't want to have this optimization at
    // all, as we try to track objects and do multiple different checks
    // on whether a given object is valid in a given context
    #ifdef DEBUG
    #define IS_SAFE_IMMORTAL(o) 0
    #else
    #define IS_SAFE_IMMORTAL(o) \
        (_Py_IsImmortal(o) && calling_state->interpreter_id != 0)
    #endif

    if (o == Py_None || o == Py_True || o == Py_False || o == Py_Ellipsis) {
        // Well-known C-defined singletons are shared between
        // sub-interpreters.
        // https://peps.python.org/pep-0554/#interpreter-isolation
        return o;
    }

    if (PyUnicode_Check(o)) {
        // Immortal strings are safe to share across
        // subinterpreters.
        if (IS_SAFE_IMMORTAL(o)) {
            return o;
        }
        // Safe to call this as it will only read the data
        // from "o", allocate a new object in the host interpreter,
        // and memcpy into it.
        PyObject *copy = _PyUnicode_Copy(o);
        TRACK(calling_state, copy);
        return copy;
    }

    if (PyLong_Check(o)) {
        if (IS_SAFE_IMMORTAL(o)) {
            return o;
        }
        // Safe for the same reasons _PyUnicode_Copy is safe.
        PyObject *copy = _PyLong_Copy((PyLongObject*)o);
        TRACK(calling_state, copy);
        return copy;
    }

    if (PyFloat_Check(o)) {
        if (IS_SAFE_IMMORTAL(o)) {
            return o;
        }
        // Safe -- just accessing the struct member.
        PyObject *copy = PyFloat_FromDouble(PyFloat_AS_DOUBLE(o));
        TRACK(calling_state, copy);
        return copy;
    }

    if (PyBytes_Check(o)) {
        if (IS_SAFE_IMMORTAL(o)) {
            return o;
        }
        // Pure copy -- allocate a new object in this thread copying
        // memory from the bytes object on the other side.
        PyObject *copy = PyBytes_FromStringAndSize(
            PyBytes_AS_STRING(o),
            PyBytes_GET_SIZE(o)
        );
        TRACK(calling_state, copy);
        return copy;
    }

    if (MEMHIVE_IS_COPYABLE(o)) {
        PyErr_SetString(PyExc_ValueError,
                        "no copy implementation");
        return NULL;
    } else if (MEMHIVE_IS_PROXYABLE(o)) {
        module_unaryfunc copyfunc =
            ((ProxyableObject*)o)->proxy_desc->make_proxy;
        PyObject *copy = (*copyfunc)(calling_state, o);
        TRACK(calling_state, copy);
        return copy;
    } else if (PyTuple_CheckExact(o)) {
        PyObject *t = PyTuple_New(PyTuple_Size(o));
        if (t == NULL) {
            return NULL;
        }
        for (Py_ssize_t i = 0; i < PyTuple_Size(o); i++) {
            PyObject *el = PyTuple_GetItem(o, i);
            assert(el != NULL);
            PyObject *oo = MemHive_CopyObject(calling_state, el);
            if (oo == NULL) {
                return NULL;
            }
            PyTuple_SetItem(t, i, oo);
        }
        TRACK(calling_state, t);
        return t;
    } else {
        PyErr_SetString(PyExc_ValueError,
                        "cannot copy an object from another interpreter");
        return NULL;
    }
}
