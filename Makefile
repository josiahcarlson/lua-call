SHELL=/bin/bash

.PHONY: clean install upload docs test

default:
	find . -type f | xargs chmod -x

clean:
	-rm -f *.pyc examples/*.pyc MANIFEST
	-rm -rf dist build

install:
	python setup.py install

upload: docs
	python setup.py sdist upload

docs:
	python -c "import lua_call; open('README.rst', 'wb').write(lua_call.__doc__)"

