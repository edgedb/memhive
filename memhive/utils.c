#include "memhive.h"


PyObject *
MemHive_CopyObject(module_state *calling_state, DistantPyObject *o)
{
    assert(o != NULL);

    if (o == Py_None || o == Py_True || o == Py_False || o == Py_Ellipsis) {
        // Well-known C-defined singletons are shared between
        // sub-interpreters.
        // https://peps.python.org/pep-0554/#interpreter-isolation
        return o;
    }

    if (PyUnicode_Check(o)) {
        // Immortal strings are safe to share across
        // subinterpreters.
        // if (_Py_IsImmortal(o)) {
        //     return o;
        // }
        // Safe to call this as it will only read the data
        // from "o", allocate a new object in the host interpreter,
        // and memcpy into it.
        return _PyUnicode_Copy(o);
    }

    if (PyLong_Check(o)) {
        // if (_Py_IsImmortal(o)) {
        //     return o;
        // }
        // Safe for the same reasons _PyUnicode_Copy is safe.
        return _PyLong_Copy((PyLongObject*)o);
    }

    if (PyFloat_Check(o)) {
        // if (_Py_IsImmortal(o)) {
        //     return o;
        // }
        // Safe -- just accessing the struct member.
        return PyFloat_FromDouble(PyFloat_AS_DOUBLE(o));
    }

    if (PyBytes_Check(o)) {
        // if (_Py_IsImmortal(o)) {
        //     Py_INCREF(o);
        //     return o;
        // }
        // Pure copy -- allocate a new object in this thread copying
        // memory from the bytes object on the other side.
        return PyBytes_FromStringAndSize(
            PyBytes_AS_STRING(o),
            PyBytes_GET_SIZE(o)
        );
    }

    if (MEMHIVE_IS_COPYABLE(o)) {
        PyErr_SetString(PyExc_ValueError,
                        "no copy implementation");
        return NULL;
    } else if (MEMHIVE_IS_PROXYABLE(o)) {
        module_unaryfunc copy = ((ProxyableObject*)o)->proxy_desc->make_proxy;
        return (*copy)(calling_state, o);
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
        return t;
    } else {
        // printf("fail\n");
        // PyObject_Print(Py_TYPE(o), stdout, 0);
        // printf("fail\n");
        PyErr_SetString(PyExc_ValueError,
                        "cannot copy an object from another interpreter");
        return NULL;
    }
}
