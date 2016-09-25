# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at

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

top_rul = {
"830057":0,
"261565":0,
"1685391":0,
"386792":0,
"541787":0,
"1092259":0,
"1212568":0,
"742539":0,
"201764":0,
"888507":0,
"349708":0,
"1396128":0,
"401168":0,
"1196479":0,
"236014":0,
"1117999":0,
"1429306":0,
"1258074":0,
"1073898":0,
"500940":0,
"685805":0,
"19155":0,
"1613443":0,
"829419":0,
"1101587":0,
"341176":0,
"1300232":0,
"738939":0,
"1397954":0,
"91779":0,
"329517":0,
"905595":0,
"878681":0,
"926352":0,
"1044626":0,
"291031":0,
"329588":0,
"1339133":0,
"521728":0,
"963411":0,
"164238":0,
"210598":0,
"719865":0,
"1514948":0,
"79281":0,
"278846":0,
"54602":0,
"976772":0,
"248381":0,
"1670129":0,
"523502":0,
"677348":0,
"1136448":0,
"657811":0,
"1349634":0,
"1603086":0,
"348931":0,
"1536863":0,
"1862997":0,
"54514":0,
"948423":0,
"1375848":0,
"1429288":0,
"334884":0,
"1265395":0,
"335719":0,
"1492895":0,
"1438572":0,
"1609826":0,
"1160669":0,
"1390441":0,
"1708195":0,
"869867":0,
"1397676":0,
"1085721":0,
"285284":0,
"1677460":0,
"635407":0,
"753343":0,
"222965":0,
"1493908":0,
"1317506":0,
"1061775":0,
"115112":0,
"846754":0,
"370864":0,
"1648776":0,
"1132083":0,
"65801":0,
"1522122":0,
"195871":0,
"396208":0,
"736513":0,
"1102559":0,
"727963":0,
"981522":0,
"323590":0,
"932445":0,
"1817035":0,
"588911":0
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
    user_id, url, time = line.strip().split()
    return '\t'.join([user_id, url]), (1.0, time, time)


def reduce_record(rec_a, rec_b):
    uv_sum_part_a = rec_a[0]
    uv_sum_part_b = rec_b[0]
    return uv_sum_part_a + uv_sum_part_b, get_min_day(rec_a[1], rec_b[1]), get_max_day(rec_a[2], rec_b[2])


def get_avg_uv(line):
    user_id, url = line[0].split()
    uv_sum = line[1][0]
    min_day = get_date_from_str(line[1][1])
    max_day = get_date_from_str(line[1][2])
    day_diff = (max_day - min_day).days + 1
    return '\t'.join([user_id, url, str(float(uv_sum) / float(day_diff))])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="ChenMenQieDanGao")
    lines = sc.textFile(sys.argv[1])
    # user_id, url, time
    res = lines.map(get_key_from_line)\
        .filter(lambda x: x[0].split()[1] in top_rul)\
        .reduceByKey(reduce_record)\
        .map(get_avg_uv)
    res.saveAsTextFile(sys.argv[2])
    sc.stop()
