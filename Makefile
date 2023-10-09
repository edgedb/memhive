build: clean
	python setup.py build_ext --inplace

clean:
	find . -name '*.so' | xargs rm && rm -rf build/	
