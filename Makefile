build: clean
	python setup.py build_ext --inplace

dev: export DEBUG_MEMHIVE = 1
dev: build

clean:
	find . -name '*.so' | xargs rm && rm -rf build/	
