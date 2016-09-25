#!/bin/bash

py_name='get_app_id_install.py'
rm -rf output
spark-submit ${py_name} file://$PWD/data file://$PWD/output && cat output/*
