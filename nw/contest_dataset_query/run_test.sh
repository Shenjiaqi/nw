#!/bin/bash

py_name='get_query_uv_desc.py'
rm -rf output
spark-submit ${py_name} file://$PWD/data file://$PWD/output && cat output/*
