#! /usr/bin/env bash

set -e
set -x
# pytest tests --disable-warnings
run-vvs-db-tests
python -W ignore::DeprecationWarning -W ignore::UserWarning tests/main.py
