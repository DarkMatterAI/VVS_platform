#! /usr/bin/env bash

set -e
set -x
run-vvs-db-tests
python -W ignore::DeprecationWarning -W ignore::UserWarning tests/main.py
