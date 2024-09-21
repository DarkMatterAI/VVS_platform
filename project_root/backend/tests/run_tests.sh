#! /usr/bin/env bash

set -e
set -x
# pytest tests --disable-warnings
python -W ignore::DeprecationWarning -W ignore::UserWarning tests/main.py
# python -u tests/main.py
