#!/bin/bash

set -e -x

# Compile wheels
PYTHON="/opt/python/${PYTHON_VERSION}/bin/python"
PIP="/opt/python/${PYTHON_VERSION}/bin/pip"
${PIP} install --upgrade setuptools pip wheel~=0.31.1
cd /io
make clean
${PYTHON} setup.py bdist_wheel

# Bundle external shared libraries into the wheels.
for whl in /io/dist/*.whl; do
    auditwheel repair $whl -w /io/dist/
    rm /io/dist/*-linux_*.whl
done

${PIP} install ${PYMODULE}[test] -f "file:///io/dist"

rm -rf /io/tests/__pycache__
