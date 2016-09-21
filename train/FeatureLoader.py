import os
import random
from multiprocessing.pool import ThreadPool
from os import listdir
from os.path import join
import sys

from scipy.sparse import csr_matrix

sys.path.append("../../")
from feature import AppUsage
from feature.UserLabel import UserLabel


class FeatureLoader:
    def __init__(self):
        pass

    def load_age_feature(self, feature_dir):
        return self.load_feature(feature_dir, 'age')

    def load_gender_feature(self, feature_dir):
        return self.load_feature(feature_dir, 'gender')

    def load_feature(self, feature_dir, category):
        feature_folders = [f for f in sorted(listdir(join(feature_dir, category)))]
        print "#####", feature_folders
        tags = []
        row_cnt = 0
        m_col_cnt = 0
        value_arr = []
        x_arr = []
        y_arr = []
        for f in feature_folders:
            c = int(f)
            class_dir = join(feature_dir, category, f)
            for file in listdir(class_dir):
                with open(join(class_dir, file), 'r') as f:
                    for line in f:
                        if random.randint(0, 10) == 0:
                            record = [float(x) * 1e7 for x in line.split(',')]
                            col_cnt = 0
                            for r in record:
                                if r > 1e-13:
                                    value_arr.append(r)
                                    x_arr.append(row_cnt)
                                    y_arr.append(col_cnt)
                                col_cnt += 1
                                m_col_cnt = col_cnt
                            tags.append(c)
                            row_cnt += 1
                print len(value_arr), len(x_arr), len(y_arr)
        return csr_matrix((value_arr, (x_arr, y_arr)), shape=(len(tags), m_col_cnt)), tags

    def scan_feature(self, feature_dir, category, handle_feature):
        feature_folders = [f for f in sorted(listdir(join(feature_dir, category)))]
        for f in feature_folders:
            c = int(f)
            class_dir = join(feature_dir, category, f)
            for file in listdir(class_dir):
                with open(join(class_dir, file), 'r') as f:
                    for line in f:
                        record = [float(x) for x in line.split(',')]
                        handle_feature(c, record)

    def cal_app_use_sum(self, file):
        open_sum = {}
        time_sum = {}
        with open(file, 'r') as f:
            for line in f:
                user_id, app_id, user_open_avg, user_time_avg = line.strip().split()
                user_open_avg = float(user_open_avg)
                user_time_avg = float(user_time_avg)
                if app_id not in open_sum:
                    open_sum[app_id] = 0.0
                open_sum[app_id] += user_open_avg

                if app_id not in time_sum:
                    time_sum[app_id] = 0.0
                time_sum[app_id] += user_time_avg


    def load_data_less_than_n(self, base_dir, feature_dir, n, type):
        user_label = UserLabel()

        user_list = {}
        if type == 'gender':
            user_list = user_label.load_gender_data_less_than(base_dir, n)
        else:
            assert type == 'age'
            user_list = user_label.load_age_data_less_than(base_dir, n)
        user_id_idx = {}
        user_category = {}
        cnt = 0
        for i in user_list:
            for j in user_list[i]:
                assert j not in user_id_idx
                user_id_idx[j] = cnt
                user_category[j] = i
                cnt += 1

        app_usage = AppUsage()
        app_list = app_usage.load_app_id(base_dir)
        cnt = 0
        app_id_idx = {}
        for i in app_list:
            app_id = i.strip().split()[0]
            assert app_id not in app_id_idx
            app_id_idx[app_id] = cnt
            cnt += 1

        feature_dir = join(base_dir, feature_dir)
        x_list = []
        y_list = []
        v_list = []

        app_open_sum = {}
        app_time_sum = {}
        file_list = []
        for file in os.listdir(feature_dir):
            file_list.append(join(feature_dir, file))

        app_id_list = []
        tag_list = [0 for i in xrange(0, len(user_category))]

        for file in file_list:
            with open(file, 'r') as f:
                for line in f:
                    user_id, app_id, user_open_avg, user_time_avg, gender, age, app_open_time, app_use_time = line.strip().split()
                    user_open_avg = float(user_open_avg)
                    user_time_avg = float(user_time_avg)

                    if app_id not in app_open_sum:
                        app_open_sum[app_id] = 0.0
                    if app_id not in app_time_sum:
                        app_time_sum[app_id] = 0.0
                    app_open_sum[app_id] += user_open_avg
                    app_time_sum[app_id] += user_time_avg

                    if user_id in user_category:
                        tag_list[user_id_idx[user_id]] = user_category[user_id]

                        x = user_id_idx[user_id]
                        y = app_id_idx[app_id] * 2

                        x_list.append(x)
                        y_list.append(y)
                        v_list.append(user_open_avg)
                        app_id_list.append(app_id)
                        x_list.append(x)
                        y_list.append(y + 1)
                        v_list.append(user_time_avg)
                        app_id_list.append(app_id)

        i = 0
        while i < len(app_id_list):
            v_list[i] /= app_open_sum[app_id_list[i]]
            v_list[i + 1] /= app_time_sum[app_id_list[i + 1]]
            i += 2

        return csr_matrix((v_list, (x_list, y_list)), shape=(len(tag_list), len(app_list) * 2)), tag_list
