#include "Python.h"
#include "errormech.h"
#include "debug.h"

#ifndef Py_BUILD_CORE
    #define Py_BUILD_CORE 1
#endif
#include "internal/pycore_frame.h"


static int
reflect_tb(PyObject *tb_list, PyObject *tb)
{
    if (tb == NULL) {
        return 0;
    }

    assert(PyTraceBack_Check(tb));

    PyTracebackObject *tt = (PyTracebackObject *)tb;

    if (reflect_tb(tb_list, (PyObject *)tt->tb_next) < 0) {
        return -1;
    }

    PyCodeObject *code = PyFrame_GetCode(tt->tb_frame);

    PyObject *ser = PyTuple_New(3);
    if (ser == NULL) {
        return -1;
    }

    PyObject *fn = code->co_filename;
    PyTuple_SET_ITEM(ser, 0, fn);
    Py_INCREF(fn);

    fn = code->co_name;
    PyTuple_SET_ITEM(ser, 1, fn);
    Py_INCREF(fn);

    ssize_t lineno = (ssize_t)tt->tb_lineno;
    if (lineno == -1) {
        lineno = PyCode_Addr2Line(code, tt->tb_lasti);
    }

    PyObject *ln = PyLong_FromSsize_t(lineno);
    if (ln == NULL) {
        Py_DECREF(ser);
        return -1;
    }
    PyTuple_SET_ITEM(ser, 2, ln);

    if (PyList_Append(tb_list, ser) < 0) {
        Py_DECREF(ser);
        return -1;
    }

    Py_DECREF(ser);
    return 0;
}


static ssize_t
reflect_error(PyObject *err, PyObject *memo, PyObject *ret)
{
    PyObject *pos = PyDict_GetItemWithError(memo, err);
    if (pos == NULL) {
        if (PyErr_Occurred()) {
            return -1;
        }
    } else {
        ssize_t p = PyLong_AsSsize_t(pos);
        #ifdef DEBUG
        if (p == -1) {
            assert(PyErr_Occurred() != NULL);
        }
        #endif
        return p;
    }

    PyObject *tb_list = NULL;

    PyObject *ser = PyDict_New();
    if (ser == NULL) {
        return -1;
    }

    PyObject *name = PyUnicode_FromString(Py_TYPE(err)->tp_name);
    if (name == NULL) {
        goto err;
    }
    int r = PyDict_SetItemString(ser, "name", name);
    Py_CLEAR(name);
    if (r < 0) {
        goto err;
    }

    PyObject *args = PyException_GetArgs(err);
    if (args == NULL) {
        goto err;
    }
    r = PyDict_SetItemString(ser, "args", args);
    Py_CLEAR(args);
    if (r < 0) {
        goto err;
    }

    tb_list = PyList_New(0);
    if (tb_list == NULL) {
        goto err;
    }

    if (reflect_tb(tb_list, PyException_GetTraceback(err)) < 0) {
        goto err;
    }


    r = PyDict_SetItemString(ser, "tb", tb_list);
    Py_CLEAR(tb_list);
    if (r < 0) {
        goto err;
    }

    PyObject *cause = PyException_GetCause(err);
    if (cause != Py_None && cause != NULL && cause != err) {
        ssize_t cp = reflect_error(cause, memo, ret);
        Py_DECREF(cause);
        if (cp < 0) {
            goto err;
        }
        PyObject *cpp = PyLong_FromSsize_t(cp);
        if (cpp < 0) {
            goto err;
        }
        r = PyDict_SetItemString(ser, "cause", cpp);
        Py_CLEAR(cpp);
        if (r < 0) {
            goto err;
        }
    }

    PyObject *context = PyException_GetContext(err);
    if (context != Py_None && context != NULL && context != err) {
        ssize_t cp = reflect_error(context, memo, ret);
        Py_DECREF(context);
        if (cp < 0) {
            goto err;
        }
        PyObject *cpp = PyLong_FromSsize_t(cp);
        if (cpp < 0) {
            goto err;
        }
        r = PyDict_SetItemString(ser, "context", cpp);
        Py_CLEAR(cpp);
        if (r < 0) {
            goto err;
        }
    }

    if (PyList_Append(ret, ser) < 0) {
        goto err;
    }

    ssize_t p = PyList_Size(ret);
    if (p < 0) {
        goto err;
    }
    p--;

    PyObject *pp = PyLong_FromSsize_t(p);
    if (pp == NULL) {
        return -1;
    }

    r = PyDict_SetItem(memo, err, pp);
    Py_DECREF(pp);
    if (r < 0) {
        goto err;
    }

    return p;

err:
    Py_XDECREF(ser);
    Py_XDECREF(tb_list);
    return -1;
}


PyObject *
MemHive_DumpError(PyObject *err)
{
    if (!PyExceptionInstance_Check(err)) {
        PyErr_Format(PyExc_ValueError, "expected an exception instance");
        return NULL;
    }

    PyObject *memo = NULL;
    PyObject *ret = NULL;

    memo = PyDict_New();
    if (memo == NULL) {
        goto err;
    }

    ret = PyList_New(0);
    if (ret == 0) {
        goto err;
    }

    if (reflect_error(err, memo, ret) < 0) {
        goto err;
    }

    Py_XDECREF(memo);
    return ret;

err:
    Py_XDECREF(memo);
    Py_XDECREF(ret);
    return NULL;
}
