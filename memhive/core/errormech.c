#include "Python.h"
#include "errormech.h"
#include "debug.h"

#include "frameobject.h"
// #ifndef Py_BUILD_CORE
//     #define Py_BUILD_CORE 1
// #endif
// #include "internal/pycore_frame.h"


static PyObject *
cleanup_args(PyObject *args)
{
    if (!PyTuple_CheckExact(args)) {
        PyErr_Format(PyExc_TypeError,
                        "expected a tuple for err->args");
        return NULL;
    }

    PyObject *new = PyTuple_New(Py_SIZE(args));
    if (new == NULL) {
        goto err;
    }

    for (ssize_t i = 0; i < Py_SIZE(args); i++) {
        PyObject *el = PyTuple_GetItem(args, i);
        assert(el != NULL);
        if (PyUnicode_Check(el)
            || PyLong_Check(el)
            || el == Py_None
            || el == Py_True
            || el == Py_False)
        {
            PyTuple_SetItem(new, i, el);
            Py_INCREF(el);
        } else {
            PyTuple_SetItem(new, i, Py_None);
        }
    }

    return new;

err:
    Py_XDECREF(new);
    return NULL;
}


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
    int is_group = _PyBaseExceptionGroup_Check(err);

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

    if (is_group) {
        PyObject *group = ((PyBaseExceptionGroupObject *)err)->excs;
        if (group != NULL)  {
            if (!PyTuple_CheckExact(group)) {
                PyErr_Format(PyExc_TypeError,
                             "expected a tuple for group->excs");
                goto err;
            }

            PyObject *ges = PyList_New(0);
            if (ges == NULL) {
                goto err;
            }

            for (ssize_t i = 0; i < Py_SIZE(group); i++) {
                PyObject *ge = PyTuple_GetItem(group, i);
                assert(ge != NULL);
                ssize_t er = reflect_error(ge, memo, ret);
                if (er < 0) {
                    goto err;
                }

                PyObject *eri = PyLong_FromSsize_t(er);
                if (eri == NULL) {
                    Py_DECREF(ges);
                    goto err;
                }

                r = PyList_Append(ges, eri);
                Py_DECREF(eri);

                if (r < 0) {
                    Py_DECREF(ges);
                    goto err;
                }
            }

            r = PyDict_SetItemString(ser, "excs", ges);
            Py_CLEAR(ges);
            if (r < 0) {
                goto err;
            }
        }
    }

    PyObject *args = PyException_GetArgs(err);
    if (args == NULL) {
        goto err;
    }
    PyObject *new_args = cleanup_args(args);
    r = PyDict_SetItemString(ser, "args", new_args);
    Py_CLEAR(new_args);
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


////////////////////////////////////////////////////////////////////////////////


int
restore_error(PyObject *ers, ssize_t index, PyObject *memo)
{
    assert(PyList_CheckExact(ers));
    assert(PyTuple_CheckExact(memo));

    PyObject *ed = PyList_GET_ITEM(ers, index);
    assert(PyDict_CheckExact(ed));

    PyObject *name = PyDict_GetItemString(ed, "name");
    if (name == NULL) {
        return -1;
    }
    PyObject *args = PyDict_GetItemString(ed, "args");
    if (name == NULL) {
        return -1;
    }
    PyObject *tbs = PyDict_GetItemString(ed, "tb");
    if (tbs == NULL) {
        return -1;
    }

    ssize_t name_l = 0;
    const char *name_s = PyUnicode_AsUTF8AndSize(name, &name_l);
    if (name_s == NULL) {
        return -1;
    }

    PyType_Slot empty_type_slots[] = {
        {0, 0},
    };

    char qual_name[1024];
    if (snprintf(qual_name, sizeof(qual_name),
                 "__subinterpreter__.%s", name_s) < 0)
    {
        PyErr_Format(PyExc_ValueError,
                     "could not prepend module name to error __name__");
        return -1;
    }

    PyType_Spec MinimalMetaclass_spec = {
        .name = qual_name,
        .basicsize = sizeof(PyHeapTypeObject),
        .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
        .slots = empty_type_slots,
    };

    PyObject *err_cls = PyType_FromMetaclass(
        NULL,
        NULL,
        &MinimalMetaclass_spec,
        PyExc_Exception
    );

    if (err_cls == NULL) {
        return -1;
    }

    PyObject *err = PyObject_Call(err_cls, args, NULL);
    Py_CLEAR(err_cls);
    if (err == NULL) {
        return -1;
    }

    PyThreadState *tstate = PyThreadState_Get();
    PyTracebackObject *tb = NULL;
    for (ssize_t i = 0; i < Py_SIZE(tbs); i++) {
        PyObject *tl = PyList_GetItem(tbs, i);
        assert(tl != NULL);
        assert(PyTuple_CheckExact(tl));
        PyCodeObject *code = PyCode_NewEmpty(
            PyUnicode_AsUTF8AndSize( // XXX
                PyTuple_GET_ITEM(tl, 0),
                &name_l),
            PyUnicode_AsUTF8AndSize( // XXX
                PyTuple_GET_ITEM(tl, 1),
                &name_l),
            0
        );
        assert(code != NULL); /// XXX

        PyFrameObject *frame = PyFrame_New(
            tstate,
            code,
            PyEval_GetGlobals(),
            0
        );
        assert(frame != NULL); // XXX

        PyTracebackObject *tb_next = PyObject_GC_New(
            PyTracebackObject, &PyTraceBack_Type);
        assert(tb_next != NULL); // XXX

        tb_next->tb_next = tb;
        tb_next->tb_frame = frame;
        tb_next->tb_lasti = 0;
        tb_next->tb_lineno = (int)PyLong_AsLong(
            PyTuple_GET_ITEM(tl, 2)
        ); // XXX

        tb = tb_next;
    }

    if (tb != NULL) {
        if (PyException_SetTraceback(err, (PyObject *)tb)) {
            return -1; // XXX
        }
    }

    PyObject *cause = PyDict_GetItemString(ed, "cause");
    if (cause != NULL) {
        ssize_t p = PyLong_AsSsize_t(cause);
        #ifdef DEBUG
        if (p == -1) {
            assert(PyErr_Occurred() != NULL);
        }
        #endif

        PyObject *cause_err = PyTuple_GET_ITEM(memo, p);
        assert(cause_err != NULL);
        Py_INCREF(cause_err);
        PyException_SetCause(err, cause_err);
    }

    PyObject *context = PyDict_GetItemString(ed, "context");
    if (context != NULL) {
        ssize_t p = PyLong_AsSsize_t(context);
        #ifdef DEBUG
        if (p == -1) {
            assert(PyErr_Occurred() != NULL);
        }
        #endif

        PyObject *ctx_err = PyTuple_GET_ITEM(memo, p);
        assert(ctx_err != NULL);
        Py_INCREF(ctx_err);
        PyException_SetContext(err, ctx_err);
    }

    PyTuple_SET_ITEM(memo, index, err);
    return 0;
}


PyObject *
MemHive_RestoreError(PyObject *ers)
{
    if (!PyList_CheckExact(ers)) {
        PyErr_Format(PyExc_ValueError, "expected a list");
        return NULL;
    }

    PyObject *memo = NULL;

    memo = PyTuple_New(Py_SIZE(ers));
    if (memo == NULL) {
        goto err;
    }

    for (ssize_t i = 0; i < Py_SIZE(ers); i++) {
        if (restore_error(ers, i, memo) < 0) {
            goto err;
        }
    }

    PyObject *err = PyTuple_GET_ITEM(memo, Py_SIZE(memo) - 1);
    assert(err != NULL);
    Py_INCREF(err);
    Py_XDECREF(memo);
    return err;

err:
    Py_XDECREF(memo);
    return NULL;
}
