module = tydb

unittest_args = -m unittest discover -s test/


test:
	python $(unittest_args)

coverage:
	coverage run $(unittest_args)
	coverage html

docs:
	pdoc $(module) -o html
