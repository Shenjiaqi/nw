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

uv_top_app_id = {
"e63ec89b2dc6b5d992ec0a4308b4e547":0,
"c42d288ff3a78bf95590a8582dd42934":0,
"b6c5ebd6df35799b493dfec4d67f3d1c":0,
"a47ad438a0030a774fd1a397f1983613":0,
"771372809b205849f48e92d249f29ad4":0,
"192b3db7416e12e150deaf52850cd1a6":0,
"8924ef427e8145c293059cd44059ecef":0,
"6aee91153e7befbe4dba67a96b75cb92":0,
"bcf05bf5c7e14231ad0645b56450e272":0,
"9294b8caef83e514021b60d3d5b64f36":0,
"c1ebe9575a371a5f64f8e4e1eb245ba2":0,
"cd223c585b980dc2342b474a780cc875":0,
"79d09728c99811a8093c23f940f11c7a":0,
"157c771e7811a409d840b3a4814def97":0,
"4180ba8037f14068b8ed39e791af02ce":0,
"ce34a233b8f9ba5f31571f48b188986e":0,
"80b0f5bd3ade42ee52919738cb53430e":0,
"8fd9ca7724679cc7c3a5a724d73f3407":0,
"90a362b28e28fe8d002cd80fabd8c158":0,
"cd626c032f239ae472635045ed7ed0eb":0,
"72eb117871a9aa2762692432bb5234f6":0,
"f1c1c825289bd49c8c31489e1ba31420":0,
"e2d03f5b1d8a8384a69ed25716bbd4ec":0,
"099e4a6a58161bfe461178f3d6d7a287":0,
"27af2f5d703b7b96a82ca859946e2368":0,
"b0be2f26e2ba7f48d503969e53784210":0,
"d638ff37e05d8b0208ea840c986b5949":0,
"9891446f5cc7949be0c954d34773a955":0,
"691ec2ed096be2a126f513f12d7e9219":0,
"d71ca17dd9ca9b4023583fbc3f21a6b2":0,
"082d34a18741cec073a95e98063a61ac":0,
"911ea194ec9edd0c282ea7a0a7d34767":0,
"be1af281fff3030030062cb0b7d41307":0,
"10ed81605c81acf15126a803b6626d7f":0,
"ef6e2bc3659fa611c4ab348472efd3d8":0,
"51c35d89618d23f488f4f5feb6c6543e":0,
"e5820cc23a13f9398df4d7b9a263b03d":0,
"164c0c7e6bb802e9d076131958dddee7":0,
"1bbb6713cd293fcdfa494d24375512a3":0,
"221e922f0f23a23ca8eda6bdd15a0c0e":0,
"68628acb02e04452e6fa7b40b24afa39":0,
"bd92dedf3ffafa006a78fdeef73c3fd0":0,
"1c05270f835bf1d926c4a42aecb672af":0,
"23fbed927a72626ac6955d7b04c96fa3":0,
"ad3641773528d5c9cc98811f88e0e155":0,
"e2874e2533df6314ad83c2638fab0668":0,
"95349c3e32424f478e11c422ec3e85ce":0,
"669a1ebb981330ff99e18f0e5f722303":0,
"a8c2164e4d993fd5a24aeb3c3316140d":0,
"da031ce8faa0b918d9f1dd732fb3f521":0,
"753dd9126792945010e0c07a2dc13cde":0,
"81bfe7cb69870fca028e628a1a97855d":0,
"284433f78e86bda95e3b8190eac8890a":0,
"4594bfacb6014275edeedba384f08348":0,
"902b55d87e9b881b6360d039d1fa22ae":0,
"aa0cd5ad7150ceee1619e726f958f6b0":0,
"0e760c47dfefd728b571002ada736864":0,
"8aa5fff887f4e0dc1216ada45f34728b":0,
"ca41b956831fc41b719ab04ebe69b8b6":0,
"63f0615566ca9ecf934db81371ec739c":0,
"7204c870ef6aeb49e5c9ecdc46cb0f50":0,
"c0d290547505954af81c2c82fa76d676":0,
"cbe005639bcf8bf4e326b20555d7c92c":0,
"e4bb299a122b3e78e4fbef9e040caa1e":0,
"ef276b8947ebd9ac21b5b005fb1d5eb4":0,
"4b88b7e2bd6aa61f63b081a2dd23c2d8":0,
"a2740dc13fac366a8b3d7a5f4beb8969":0,
"753b4372d2ef3d36967e4b19cec901a2":0,
"9d397d5391e4b1d9c79eb2441ba2f110":0,
"930c699a6e6e7a90359787e030b3918a":0,
"46cbc560092d1de45e6b7ff75bf41ab7":0,
"eb72a6495977d99f5a67ac9948ccb4d4":0,
"c58e1b4f012c571efd7b61788296864c":0,
"1400f33020d05ee7c591b4e9318e60f0":0,
"621492012b7b599028b7ac2363a86d0d":0,
"e4818145d5b489c6c7bc0220dfd87a66":0,
"424b551adfdbd217d123fefea55a5edb":0,
"08eace5cb8ce81d5aba69d491df0cdcb":0,
"f35da8085284ed416fc07b59802cd443":0,
"0c7f3af6f7607c27a5bfa16d2dd25207":0,
"bf0a408eb5d837408c1a0bc356269bfe":0,
"ad23730b08fadd3a859f6789afaa95f6":0,
"6e017ab1ba9321809e1dd8b44d3cb2fc":0,
"fd0eb400c68f48a758b1c874bdc072e1":0,
"d45bbfb3c55c91ff69e68f11fdae945b":0,
"ee34791b8dde6993121e14a20226a07c":0,
"c97f7dbcbfc2928b2503d9faab6ff9ec":0,
"165db78ca430f113215b15a9991ac85f":0,
"1fadf80f7826a3740286e6a4436e841f":0,
"4fb8885b94447b1a7a1700129e6deec0":0,
"cea7e860cf086f3af01922cff27a75f8":0,
"855b29edd5770271a5467490729525bc":0,
"ebbb659dff5f5d964e3649a6c8f8b16b":0,
"0ad269dac2558b83d9bb149caf172e07":0,
"2edd4c1f1435f2d07e41b78bc2d0754c":0,
"98bb2d16600c377c7cae78d6ad8b4eca":0,
"6302c9281b203c03a17b82cb6f757f1b":0,
"00a66ec1307573415ae66c1ecfbd528e":0,
"69dc69364a2aa910bb18cddc07ba8851":0,
"3c81c85893b04d6b30e7e0b08a538a16":0,
"79d09728c99811a8093c23f940f11c7a":0,
"099e4a6a58161bfe461178f3d6d7a287":0,
"aee54e9654e265b61bf5ed6f3528fdce":0,
"eb3b0ad007c11399237bb88fbfdddb0e":0,
"23fbed927a72626ac6955d7b04c96fa3":0,
"157c771e7811a409d840b3a4814def97":0,
"ef6e2bc3659fa611c4ab348472efd3d8":0,
"c42d288ff3a78bf95590a8582dd42934":0,
"a47ad438a0030a774fd1a397f1983613":0,
"0a4e3296675bc38a2c896ad23842ae36":0,
"26637ffd70d8613a7fb889ef97e6c76d":0,
"669a1ebb981330ff99e18f0e5f722303":0,
"6eddd1e5fb1e55fe2aaec8fcf3f9c4f8":0,
"396adb886b66d2f4f4c728aae7ff5bc8":0,
"100bfcee8d6d17fb1ad7b1d879c45ac2":0,
"cd626c032f239ae472635045ed7ed0eb":0,
"f83f5c633bfa99766de9ac9d76904a09":0,
"4e6046e0d1395686bc2b7f9ff9f97ae6":0,
"930c699a6e6e7a90359787e030b3918a":0,
"7e3aeca5a48e2f83520a4b756aab900a":0,
"181bbc34d2437a9e4391eec991c46e5e":0,
"bcc4bec8fb2f77b6e3ec286013a06ee9":0,
"771372809b205849f48e92d249f29ad4":0,
"bf62ccf1f729032e703bb5c7f94175e1":0,
"e1464ba94a6e5105761017177236cbaa":0,
"855b29edd5770271a5467490729525bc":0,
"c2b5803e719267f57f33c301ad234838":0,
"fa538f3796672d24156ee66159d972c8":0,
"d4db6c33ef974befcadd2fb34e92b42d":0,
"59cc3da951ace74c01ae8b18a6ecb0fc":0,
"605132d0b6c409c981d9ab5608a20495":0,
"ed714f2bce66c9bb5017036b55c493b2":0,
"8d3f271840e35bf1250462d32ab361a4":0,
"252421dca709b5fced074b225b23aa0c":0,
"e1911830d2e63ce355cd9949a1c7a439":0,
"8aa5fff887f4e0dc1216ada45f34728b":0,
"098858b017b22bd172908bbd4feea10c":0,
"97ed5e35c2ff100342e87cf6ac1dec3e":0,
"4628bc6f482cb994c434b94bb6afdfcc":0,
"3dad728e60151f2e19b09f3abadc0020":0,
"e954fe1945dc4e83217d6276c6af9f3b":0,
"f1c1c825289bd49c8c31489e1ba31420":0,
"8fd9ca7724679cc7c3a5a724d73f3407":0,
"bf0a408eb5d837408c1a0bc356269bfe":0,
"a2740dc13fac366a8b3d7a5f4beb8969":0,
"ee34791b8dde6993121e14a20226a07c":0,
"8ad760350b09ba1a25243023cb503dbe":0,
"d638ff37e05d8b0208ea840c986b5949":0,
"f354775007e1fdb1a8eee3c79cc960a8":0,
"397d6e54718af619d3060ba82d6a3542":0,
"72eb117871a9aa2762692432bb5234f6":0,
"81bfe7cb69870fca028e628a1a97855d":0,
"f96e59eb7c2c1ef3b0dbc39e867649e4":0,
"94f7809cccdbab8f125affa6e261427a":0,
"d4d434b3cacf1f7010e03375b3396e57":0,
"ace3f01e834cebfbcfb2998da7f7ed47":0,
"23bc5510906d5c19d5ca5446c4f27bce":0,
"e9912462b9eea8de958d321e4644c558":0,
"cea7e860cf086f3af01922cff27a75f8":0,
"f7cd02aaee99800e0a697d9ca27d8745":0,
"99dced435b80432181d7c6fb4a753d53":0,
"0e760c47dfefd728b571002ada736864":0,
"f6fd1b3d649641d0a2117a5f1b922a96":0,
"f2e5e24fb1f9a117b80d7cf237a27fdd":0,
"9891446f5cc7949be0c954d34773a955":0,
"2cbc2bb56b61375b172f1611e9b98463":0,
"c8591a918a2b03d053d8f8ad711b84e7":0,
"b0be2f26e2ba7f48d503969e53784210":0,
"406d0c420ccf4db96fb80d2b76777a0d":0,
"ac0508078a7cffa85dd230767c9bbf8b":0,
"c97f7dbcbfc2928b2503d9faab6ff9ec":0,
"691ec2ed096be2a126f513f12d7e9219":0,
"d71ca17dd9ca9b4023583fbc3f21a6b2":0,
"1bbb6713cd293fcdfa494d24375512a3":0,
"424b551adfdbd217d123fefea55a5edb":0,
"ca41b956831fc41b719ab04ebe69b8b6":0,
"165db78ca430f113215b15a9991ac85f":0,
"164c0c7e6bb802e9d076131958dddee7":0,
"a34aafc1fd1658fccde4f18af4056364":0,
"753b4372d2ef3d36967e4b19cec901a2":0,
"cbe005639bcf8bf4e326b20555d7c92c":0,
"ac8b308979610164afdc685a8cbbcf77":0,
"8790a2041b3c591c4a1b0c1637203e2b":0,
"e5aa5dde5f2da75c993f00b2ca872dbc":0,
"2745e074fac3604400f365c28f60e654":0,
"aa70db4400720a1188d4eb9713380f27":0,
"a2d8348c49d8cc69bdfcf177abf2bdb3":0,
"95a294a9b7836fb52a5c8e459644a259":0,
"b814e5217f5f6d0d8c94d272678c7250":0,
"aa0cd5ad7150ceee1619e726f958f6b0":0,
"4c4d92111d76b5f87cc98970314ebc4a":0,
"51c35d89618d23f488f4f5feb6c6543e":0,
"10ed81605c81acf15126a803b6626d7f":0,
"e5820cc23a13f9398df4d7b9a263b03d":0,
"27af2f5d703b7b96a82ca859946e2368":0,
"6e2ea79a09696cb8aae6b96c42f93198":0,
"7f8be68b5f1af97eacb0a0bc5d7c362a":0,
"dc854849ad9f464171c6b31c7d634e71":0,
"002be9016d5af8ce1cd94d5205240152":0,
"7204c870ef6aeb49e5c9ecdc46cb0f50":0
}

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
    return '\t'.join([user_id, app_id]), (float(count), float(duration), time, time)

def reduce_record(rec_a, rec_b):
    count_a, duration_a, time_min_a, time_max_a = rec_a
    count_b, duration_b, time_min_b, time_max_b = rec_b
    return count_a + count_b, duration_a + duration_b, get_min_day(time_min_a, time_min_b), get_max_day(time_max_a, time_max_b)

def get_avg_uv(line):
    user_id, app_id = line[0].split()
    count = line[1][0]
    duration = line[1][1]
    min_day = get_date_from_str(line[1][2])
    max_day = get_date_from_str(line[1][3])
    day_diff = float((max_day - min_day).days + 1.0)
    return '\t'.join([user_id, app_id, str(count / day_diff), str(duration / day_diff)])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="ChenMenQieDanGao")
    lines = sc.textFile(sys.argv[1])
    # user_id, app_id, count, duration, time
    res = lines.map(get_key_from_line)\
        .filter(lambda x: x[0].split()[1] in uv_top_app_id)\
        .reduceByKey(reduce_record)\
        .map(get_avg_uv)
    res.saveAsTextFile(sys.argv[2])
    sc.stop()
