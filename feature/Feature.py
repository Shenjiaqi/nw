import json

import datetime
import os
from os.path import join

import AppUsage
import UserLabel
import shutil

class Feature:
    def __init__(self, conf):
        self.app_usage = AppUsage.AppUsage()
        self.user_label = UserLabel.UserLabel()
        self.conf = conf
        self.base_dir = conf['base_dir']
        self.feature_dir = join(self.base_dir, conf['feature_dir'])

    def load_data(self):
        base_dir = self.conf['base_dir']
        self.app_usage.load_data_from_base_dir(base_dir=base_dir)
        self.user_label.load_data_from_base_dir(base_dir=base_dir)

    def process_record(self, user_id, app_id, count, duration, date):
        if user_id in self.user_id_dict and app_id in self.topk_appid_dict:
            if user_id not in self.rec:
                self.rec[user_id] = {}
            if app_id not in self.rec[user_id]:
                self.rec[user_id][app_id] = {
                    'count_sum': 0,
                    'duration_sum': 0,
                    'day_cnt': 0
                }
            self.rec[user_id][app_id]['count_sum'] += count
            self.rec[user_id][app_id]['duration_sum'] += duration
            self.rec[user_id][app_id]['day_cnt'] += 1

    def process_record_end_of_one_file(self, origin_file_name):
        # write data in rec to file, clean rec

        gender_dir = join(self.feature_dir, 'gender')
        age_dir = join(self.feature_dir, 'age')
        for i in [gender_dir, age_dir]:
            if not os.path.exists(i):
                os.makedirs(i)
        for user in self.rec:
            feature_line = []
            for app_id in self.topk_appid_dict:
                avg_open_cnt_per_day = 0.0
                avg_use_duration_per_day = 0.0
                if app_id in self.rec[user]:
                    days = float(self.rec[user][app_id]['day_cnt'])
                    avg_open_cnt_per_day = float(self.rec[user][app_id]['count_sum']) / days
                    avg_use_duration_per_day = float(self.rec[user][app_id]['duration_sum']) / days
                feature_line.append(avg_open_cnt_per_day)
                feature_line.append(avg_use_duration_per_day)
            # write feature line
            user_info = self.user_label.get_user(user_id=user)
            user_gender = user_info['gender']
            user_age_group = user_info['age_group']
            with open(join(gender_dir, str(user_gender), 'feature'), 'a') as f:
                f.write(','.join([str(i) for i in feature_line]) + '\n')
            with open(join(age_dir, str(user_age_group), 'feature'), 'a') as f:
                f.write(','.join([str(i) for i in feature_line]) + '\n')
        self.rec = {}

    def generate_user_feature_by_topk_open_appid(self, k):
        # clear feature dir
        if os.path.exists(self.feature_dir):
            shutil.rmtree(self.feature_dir)

        print "get topk start", datetime.datetime.now()
        self.topk_appid_dict = self.app_usage.get_topk_open_appid(k)
        print "get topk end", datetime.datetime.now()

        print "get user id list start", datetime.datetime.now()
        user_id_list = self.user_label.get_user_list()
        print "get user id list end", datetime.datetime.now()
        self.user_id_dict = {}
        for u in user_id_list:
            self.user_id_dict[u] = None

        print "extrace record start", datetime.datetime.now()
        self.rec = {}
        self.app_usage.scan_record(process_record=self.process_record,
                                   on_end_of_one_file=self.process_record_end_of_one_file)
        print "extrace record end", datetime.datetime.now()
        # {user_id: {app_id: {duration, day_sum, open_sum}}}

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        feature = Feature(conf=conf)
        feature.load_data()
        feature.generate_user_feature_by_topk_open_appid(conf['app_usage_topK'])
