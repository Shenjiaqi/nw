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

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="ChenMenQieDanGao")
    lines = sc.textFile(sys.argv[1])
    # user_id, app_id
    res = lines.map(lambda x: (x.strip().split()[1], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda a: -a[1])\
        .map(lambda a: '\t'.join([a[0], str(a[1])]))
    res.saveAsTextFile(sys.argv[2])
    ##counts = lines.map(lambda x: (x, x)).groupByKey().map(lambda x: x)

    sc.stop()
