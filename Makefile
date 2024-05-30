.PHONY: build test clean all testinstalled


PYTHON ?= python
ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

all: dev

build: clean
	python setup.py build_ext --inplace

dev: export DEBUG_MEMHIVE = 1
dev: build

clean:
	find . -name '*.so' | xargs rm && rm -rf build/

test:
	$(PYTHON) -m pytest -v

testinstalled:
	cd /tmp && $(PYTHON) $(ROOT)/tests/__init__.py
