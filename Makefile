module = tydb

unittest_args = -m unittest discover -s tests/
coverage_args = --omit tests/utils.py


test:
	python $(unittest_args)

coverage:
	coverage erase
	coverage run $(coverage_args) $(unittest_args)
	coverage html

docs:
	pdoc $(module) -o html
