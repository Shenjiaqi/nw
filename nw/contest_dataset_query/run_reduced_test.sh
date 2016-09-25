#!/bin/bash

py_name='get_reduced_query.py'
rm -rf output
spark-submit ${py_name} file://$PWD/reduced_data file://$PWD/output && cat output/*
