#include "memhive.h"


PyObject *
MemHive_CopyObject(DistantPyObject *o)
{
    assert(o != NULL);

    if (o == Py_None || o == Py_True || o == Py_False || o == Py_Ellipsis) {
        // Well-known C-defined singletons are shared between
        // sub-interpreters.
        // https://peps.python.org/pep-0554/#interpreter-isolation
        Py_INCREF(o);
        return o;
    }

    if (PyUnicode_Check(o)) {
        // Immortal & immutable objects are safe to share across
        // subinterpreters.
        if (_Py_IsImmortal(o) || PyUnicode_CHECK_INTERNED(o)) {
            Py_INCREF(o);
            return o;
        }
        // Safe to call this as it will only read the data
        // from "o", allocate a new object in the host interpreter,
        // and memcpy into it.
        return _PyUnicode_Copy(o);
    }

    if (PyLong_Check(o)) {
        if (_Py_IsImmortal(o)) {
            Py_INCREF(o);
            return o;
        }
        // Safe for the same reasons _PyUnicode_Copy is safe.
        return _PyLong_Copy((PyLongObject*)o);
    }

    if (PyFloat_Check(o)) {
        if (_Py_IsImmortal(o)) {
            Py_INCREF(o);
            return o;
        }
        // Safe -- just accessing the struct member.
        return PyFloat_FromDouble(PyFloat_AS_DOUBLE(o));
    }

    if (PyBytes_Check(o)) {
        if (_Py_IsImmortal(o)) {
            Py_INCREF(o);
            return o;
        }
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
    } else {
        PyErr_SetString(PyExc_ValueError,
                        "cannot copy an object from another interpreter");
        return NULL;
    }
}
