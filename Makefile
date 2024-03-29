module = tydb

unittest_args = -m unittest discover -s tests/


test:
	python $(unittest_args)

coverage:
	coverage erase
	coverage run $(unittest_args)
	coverage html

docs:
	pdoc $(module) -o html
