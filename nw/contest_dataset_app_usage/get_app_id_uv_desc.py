#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
import math
from datetime import datetime

from pyspark import SparkContext

date_format = "%Y-%m-%d"

def get_date_from_str(s):
    return datetime.strptime(s, date_format)

def get_min_day(day_1, day_2):
    d_1 = get_date_from_str(day_1)
    d_2 = get_date_from_str(day_2)
    return day_1 if d_1 < d_2 else day_2

def get_max_day(day_1, day_2):
    d_1 = get_date_from_str(day_1)
    d_2 = get_date_from_str(day_2)
    return day_1 if d_1 > d_2 else day_2
   
def get_key_from_line(line):
    user_id, app_id, count, duration, time = line.strip().split()
    return '\t'.join([user_id, app_id, time])

def get_key_from_user_app_time(line):
    ueser_id, app_id, time = line.split()
    return ('\t'.join([app_id, time]), 1.0)

def reduce_uv(rec_a, rec_b):
    uv_sum_part_a = rec_a[0]
    uv_sum_part_b = rec_b[0]
    return (uv_sum_part_a + uv_sum_part_b, get_min_day(rec_a[1], rec_b[1]), get_max_day(rec_a[2], rec_b[2]))

def get_key_from_app_time(line):
    app_id, time = line[0].split()
    uv = line[1]
    return (app_id, (uv, time, time))

def get_avg_uv(line):
    app_id = line[0]
    uv_sum = line[1][0]
    min_day = get_date_from_str(line[1][1])
    max_day = get_date_from_str(line[1][2])
    day_diff = (max_day - min_day).days + 1
    if day_diff == 0:
        assert uv_sum == 0
        return app_id, 0
    return app_id, float(uv_sum) / float(day_diff)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="ChenMenQieDanGao")
    lines = sc.textFile(sys.argv[1])
    # user_id, app_id, count, duration, time
    res = lines.map(get_key_from_line)\
        .distinct()\
        .map(get_key_from_user_app_time)\
        .reduceByKey(lambda a, b: a + b)\
        .map(get_key_from_app_time)\
        .reduceByKey(reduce_uv)\
        .map(get_avg_uv)\
        .sortBy(lambda a: -a[1])\
        .map(lambda a: '\t'.join([a[0], str(a[1])]))
    res.saveAsTextFile(sys.argv[2])
    ##counts = lines.map(lambda x: (x, x)).groupByKey().map(lambda x: x)

    sc.stop()
