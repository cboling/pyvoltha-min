# Copyright 2018 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configure shell
SHELL = bash -eu -o pipefail

THIS_MAKEFILE	:= $(abspath $(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))
WORKING_DIR		:= $(dir $(THIS_MAKEFILE) )
PACKAGE_DIR     := $(WORKING_DIR)pyvoltha_min
VENVDIR			:= venv-pyvoltha-min
TESTVENVDIR		:= ${VENVDIR}-test
VENV_BIN		?= virtualenv
VENV_OPTS		?= --python=python3.6 -v
# VENV_OPTS		?= --python=python3.8 -v    NOTE: Still stay on 3.6 for the time being until full regression performed
COVERAGE_OPTS	= --with-xcoverage --with-xunit --cover-package=pyvoltha-min\
                  --cover-html --cover-html-dir=tmp/cover
PYLINT_OUT		= $(WORKING_DIR)pylint.out

default: help

# This should to be the first and default target in this Makefile
help:
	@echo "Usage: make [<target>]"
	@echo "where available targets are:"
	@echo
	@echo "help          : Print this help"
	@echo "test            : Run all unit test"
	@echo "lint            : Run pylint on package"
	@echo
	@echo "dist          : Create source distribution of the python package"
	@echo "check         : run twine check on distribution"
	@echo "upload        : Upload test version of python package to test.pypi.org"
	@echo
	@echo "venv          : Build local Python virtualenv"
	@echo "venv-test     : Build local Python unit test and lint virtualenv"
	@echo
	@echo "show-licenses   : Show imported modules and licenses"
	@echo "bandit-test     : Run bandit security test on package code"
	@echo "bandit-test-all : Run bandit security test on package and imported code"
	@echo
	@echo "clean         : Remove files created by the build and tests"
	@echo "distclean     : Remove files created by the build and tests and virtual environments"

# ignore these directories
.PHONY: test dist

local-protos:
	mkdir -p local_imports
ifdef LOCAL_PROTOS
	mkdir -p local_imports/voltha-protos/dist
	rm -f local_imports/voltha-protos/dist/*.tar.gz
	cp ${LOCAL_PROTOS}/dist/*.tar.gz local_imports/voltha-protos/dist/
endif

dist:
	@ echo "Creating python source distribution"
	rm -rf dist/
	python setup.py sdist

upload: dist check
	@ echo "Uploading sdist to test.pypi.org"
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

venv: local-protos
	@ VIRTUAL_ENV_DISABLE_PROMPT=true $(VENV_BIN) ${VENV_OPTS} ${VENVDIR};\
        source ./${VENVDIR}/bin/activate ; set -u ;\
        pip install -r requirements.txt

ifdef LOCAL_PROTOS
	source ./${VENVDIR}/bin/activate ; set -u ;\
	pip install local_imports/voltha-protos/dist/*.tar.gz
endif

venv-test: local-protos
	@ VIRTUAL_ENV_DISABLE_PROMPT=true $(VENV_BIN) ${VENV_OPTS} ${TESTVENVDIR};\
        source ./${TESTVENVDIR}/bin/activate ; set -u ;\
        pip install -r test/requirements.txt

ifdef LOCAL_PROTOS
	source ./${TESTVENVDIR}/bin/activate ; set -u ;\
	pip install local_imports/voltha-protos/dist/*.tar.gz
endif

######################################################################
# Test support

test: clean
	@ echo "Executing unit tests w/tox"
	tox

######################################################################
# License and security checks support

show-licenses:
	@ (. ${VENVDIR}/bin/activate && \
       pip install pip-licenses && \
       pip-licenses)

bandit-test:
	@ echo "Running python security check with bandit on module code"
	@ (. ${TESTVENVDIR}/bin/activate && pip install bandit && bandit -n 3 -r $(PACKAGE_DIR))

bandit-test-all: venv bandit-test
	@ echo "Running python security check with bandit on imports"
	@ (. ${TESTVENVDIR}/bin/activate && bandit -n 3 -r ${VENVDIR})

######################################################################
# pylint support

lint: clean
	@ echo "Executing pylint"
	@ . ${TESTVENVDIR}/bin/activate && $(MAKE) lint-pyvoltha-min

lint-pyvoltha-min:
	- pylint --rcfile=${PACKAGE_DIR}/.pylintrc ${PACKAGE_DIR} 2>&1 | tee ${WORKING_DIR}pylint.out.txt
	@ echo
	@ echo "See \"file://${WORKING_DIR}pylint.out.txt\" for lint report"
	@ echo

######################################################################
# Linting of the README.rst

lint-rst:
	rst-lint --level info README.rst

check:
	twine check dist/*

######################################################################
# Cleanup

clean:
	@ find . -name '*.pyc' | xargs rm -f
	@ find . -name '__pycache__' | xargs rm -rf
	@ -find . -name 'htmlcov' | xargs rm -rf
	@ -find . -name 'junit-report.xml' | xargs rm -rf
	@ -find . -name 'pylint.out.*' | xargs rm -rf
	@ rm -rf \
    .tox \
    .coverage \
    coverage.xml \
    dist \
    nose-results.xml \
    *.egg-info \
    test/unit/tmp \
    local_imports

distclean: clean
	rm -rf ${VENVDIR}
	rm -rf ${TESTVENVDIR}

distclean-test: clean
	rm -rf ${TESTVENVDIR}

# end file
