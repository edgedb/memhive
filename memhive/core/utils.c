#include <string.h>

#include "memhive.h"
#include "debug.h"
#include "track.h"


PyObject *
MemHive_CopyString(PyObject *o)
{
#ifdef _PyUnicode_Copy
    return _PyUnicode_Copy(o);
#else
    PyObject *copy = PyUnicode_New(
        PyUnicode_GET_LENGTH(o),
        PyUnicode_MAX_CHAR_VALUE(o)
    );
    if (copy == NULL) {
        return NULL;
    }
    memcpy(
        PyUnicode_DATA(copy), PyUnicode_DATA(o),
        PyUnicode_GET_LENGTH(o) * PyUnicode_KIND(o)
    );
    return copy;
#endif
}


PyObject *
MemHive_CopyObject(module_state *state, RemoteObject *ro)
{
    assert(ro != NULL);

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
        (_Py_IsImmortal(o) && state->interpreter_id != 0)
    #endif

    // We want to have the signature of the function to signal that
    // the object to copy must be remote; but we have no use of that
    // here, so just cast to PyObject*.
    PyObject *o = (PyObject *)ro;

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
        PyObject *copy = MemHive_CopyString(o);
        TRACK(state, copy);
        return copy;
    }

    if (PyLong_Check(o)) {
        if (IS_SAFE_IMMORTAL(o)) {
            return o;
        }
        // Safe for the same reasons _PyUnicode_Copy is safe.
        PyObject *copy = _PyLong_Copy((PyLongObject*)o);
        TRACK(state, copy);
        return copy;
    }

    if (PyFloat_Check(o)) {
        if (IS_SAFE_IMMORTAL(o)) {
            return o;
        }
        // Safe -- just accessing the struct member.
        PyObject *copy = PyFloat_FromDouble(PyFloat_AS_DOUBLE(o));
        TRACK(state, copy);
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
        TRACK(state, copy);
        return copy;
    }

    if (MEMHIVE_IS_COPYABLE(o)) {
        PyErr_SetString(PyExc_ValueError,
                        "no copy implementation");
        return NULL;
    } else if (MEMHIVE_IS_PROXYABLE(o)) {
        module_unaryfunc copyfunc =
            state->interpreter_id == 0
                ?
                    ((ProxyableObject*)o)->proxy_desc->copy_from_sub_to_main
                :
                    ((ProxyableObject*)o)->proxy_desc->copy_from_main_to_sub;
        PyObject *copy = (*copyfunc)(state, o);
        TRACK(state, copy);
        return copy;
    } else if (PyTuple_CheckExact(o)) {
        PyObject *t = PyTuple_New(PyTuple_Size(o));
        if (t == NULL) {
            return NULL;
        }
        for (Py_ssize_t i = 0; i < PyTuple_Size(o); i++) {
            RemoteObject *el = (RemoteObject*)PyTuple_GetItem(o, i);
            assert(el != NULL);
            PyObject *oo = MemHive_CopyObject(state, el);
            if (oo == NULL) {
                return NULL;
            }
            PyTuple_SetItem(t, i, oo);
        }
        TRACK(state, t);
        return t;
    } else {
        PyErr_SetString(PyExc_ValueError,
                        "cannot copy an object from another interpreter");
        return NULL;
    }
}
