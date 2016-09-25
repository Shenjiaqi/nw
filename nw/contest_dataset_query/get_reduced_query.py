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
from operator import add
from datetime import datetime

from pyspark import SparkContext

date_format = "%Y-%m-%d"

top_query = {
"2987367":0,
"1883163":0,
"1930354":0,
"2275232":0,
"2049042":0,
"1498136":0,
"3180200":0,
"2248263":0,
"1929858":0,
"2723706":0,
"1802377":0,
"1907433":0,
"2011447":0,
"2377081":0,
"1420758":0,
"1437518":0,
"1883105":0,
"2057976":0,
"2510764":0,
"3349961":0,
"1675016":0,
"2716662":0,
"3384665":0,
"2165168":0,
"2309814":0,
"2059817":0,
"2827067":0,
"1036941":0,
"1808407":0,
"1099506":0,
"1436956":0,
"3003204":0,
"1929272":0,
"1912228":0,
"3269031":0,
"3555718":0,
"3160236":0,
"1799234":0,
"2827254":0,
"1567109":0,
"2326522":0,
"1421784":0,
"2845689":0,
"1887006":0,
"2961651":0,
"2881718":0,
"1877238":0,
"3396832":0,
"2015155":0,
"1883533":0,
"1744089":0,
"2275163":0,
"1469028":0,
"2808721":0,
"1578684":0,
"1420603":0,
"2231506":0,
"1567132":0,
"475189":0,
"2950787":0,
"2593917":0,
"1915851":0,
"2698221":0,
"2377098":0,
"2688696":0,
"1923365":0,
"1479709":0,
"2971725":0,
"1513502":0,
"1908403":0,
"3004880":0,
"1688178":0,
"1293930":0,
"2827186":0,
"3045571":0,
"2567177":0,
"2763199":0,
"1929271":0,
"2827208":0,
"2930164":0,
"2239587":0,
"2859822":0,
"1955860":0,
"2723665":0,
"3278109":0,
"3398544":0,
"2800971":0,
"1388460":0,
"1679378":0,
"2315757":0,
"1808289":0,
"2745937":0,
"1975504":0,
"2709445":0,
"2321614":0,
"3198207":0,
"3406299":0,
"3628981":0,
"325467":0,
"2403561":0
}


def get_date_from_str(s):
    return datetime.strptime(s.strip(), date_format)


def get_min_day(day_1, day_2):
    d_1 = get_date_from_str(day_1)
    d_2 = get_date_from_str(day_2)
    return day_1 if d_1 < d_2 else day_2


def get_max_day(day_1, day_2):
    d_1 = get_date_from_str(day_1)
    d_2 = get_date_from_str(day_2)
    return day_1 if d_1 > d_2 else day_2


def get_key_from_line(line):
    user_id, queries, time = line.strip().split()
    l = []
    for query in queries.split(','):
        if query in top_query:
            l.append('\t'.join([user_id, query, time]))
    return l


def get_key_from_user_id_query_time(line):
    user_id, query, time = line.split()
    return '\t'.join([user_id, query]), (1.0, time, time)


def get_key_from_query_time(line):
    query, time = line[0].split()
    uv = line[1]
    return query, (uv, time, time)


def reduce_record(rec_a, rec_b):
    uv_sum_part_a = rec_a[0]
    uv_sum_part_b = rec_b[0]
    return uv_sum_part_a + uv_sum_part_b, get_min_day(rec_a[1], rec_b[1]), get_max_day(rec_a[2], rec_b[2])


def get_avg_uv(line):
    user_id_query = line[0]
    uv_sum = line[1][0]
    min_day = get_date_from_str(line[1][1])
    max_day = get_date_from_str(line[1][2])
    day_diff = (max_day - min_day).days + 1
    return '\t'.join([user_id_query, str(float(uv_sum) / float(day_diff))])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="ChenMenQieDanGao")
    lines = sc.textFile(sys.argv[1])
    # user_id, url, time
    res = lines.flatMap(get_key_from_line) \
        .map(get_key_from_user_id_query_time) \
        .reduceByKey(reduce_record) \
        .map(get_avg_uv) \

    res.saveAsTextFile(sys.argv[2])
    sc.stop()
