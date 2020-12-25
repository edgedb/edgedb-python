.PHONY: compile debug test quicktest clean all gen-errors gen-types _touch


PYTHON ?= python
ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))


all: compile


clean:
	rm -fr $(ROOT)/dist/
	rm -fr $(ROOT)/doc/_build/
	rm -fr $(ROOT)/edgedb/pgproto/*.c
	rm -fr $(ROOT)/edgedb/pgproto/*.html
	rm -fr $(ROOT)/edgedb/pgproto/codecs/*.html
	rm -fr $(ROOT)/edgedb/protocol/*.c
	rm -fr $(ROOT)/edgedb/protocol/*.html
	rm -fr $(ROOT)/edgedb/protocol/*.so
	rm -fr $(ROOT)/edgedb/datatypes/*.so
	rm -fr $(ROOT)/edgedb/datatypes/datatypes.c
	rm -fr $(ROOT)/build
	rm -fr $(ROOT)/edgedb/protocol/codecs/*.html
	find . -name '__pycache__' | xargs rm -rf


_touch:
	rm -fr $(ROOT)/edgedb/datatypes/datatypes.c
	rm -fr $(ROOT)/edgedb/protocol/protocol.c
	find $(ROOT)/edgedb/protocol -name '*.pyx' | xargs touch
	find $(ROOT)/edgedb/datatypes -name '*.pyx' | xargs touch
	find $(ROOT)/edgedb/datatypes -name '*.c' | xargs touch


compile: _touch
	$(PYTHON) setup.py build_ext --inplace


gen-errors:
	edb gen-errors --import "$(echo "from edgedb.errors._base import *"; echo "from edgedb.errors.tags import *")" \
		--extra-all "_base.__all__" --stdout --client > $(ROOT)/.errors
	mv $(ROOT)/.errors $(ROOT)/edgedb/errors/__init__.py
	$(PYTHON) tools/gen_init.py


gen-types:
	edb gen-types --stdout > $(ROOT)/edgedb/protocol/codecs/edb_types.pxi


debug: _touch
	EDGEDB_DEBUG=1 $(PYTHON) setup.py build_ext --inplace


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) setup.py test
	$(PYTHON) setup.py test
	USE_UVLOOP=1 $(PYTHON) setup.py test


testinstalled:
	cd /tmp && $(PYTHON) $(ROOT)/tests/__init__.py


quicktest:
	$(PYTHON) setup.py test


htmldocs:
	$(PYTHON) setup.py build_ext --inplace
	$(MAKE) -C docs html SPHINXOPTS="-W -n"
