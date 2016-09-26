import json
import os
import random
import shutil
from math import sqrt
from multiprocessing import Pool
from sklearn import preprocessing
from os.path import join

import datetime

import numpy
from numpy import zeros
from scipy.sparse import csr_matrix

from AppUsage import AppUsage
from FearureIdxGen import FeatureIdxGen
from UserLabel import UserLabel


class FeatureEvalGen:
    def __init__(self, conf):
        self.base_dir = conf['base_dir']

        self.output_feature_base = conf['output_feature_base']
        self.test_data_dir = join(self.base_dir, conf['test_data_dir'])
        self.query_dir = join(self.base_dir, conf['contest_dataset_query'])
        self.url_dir = join(self.base_dir, conf['contest_dataset_url'])
        self.device_info_dir = join(self.base_dir, conf['contest_dataset_device_info'])

        self.gender_train_output_feature_dir = join(self.output_feature_base, conf['gender_train_output_feature_dir'])
        self.gender_eval_output_feature_dir = join(self.output_feature_base, conf['gender_eval_output_feature_dir'])
        self.gender_submit_output_feature_dir = join(self.output_feature_base, conf['gender_submit_output_feature_dir'])

        self.age_train_output_feature_dir = join(self.output_feature_base, conf['age_train_output_feature_dir'])
        self.age_eval_output_feature_dir = join(self.output_feature_base, conf['age_eval_output_feature_dir'])
        self.age_submit_output_feature_dir = join(self.output_feature_base, conf['age_submit_output_feature_dir'])

        self.output_feature_dir = [[self.gender_train_output_feature_dir, self.gender_eval_output_feature_dir,
                                    self.gender_submit_output_feature_dir],
                                   [self.age_train_output_feature_dir, self.age_eval_output_feature_dir,
                                    self.age_submit_output_feature_dir]]

        self.feature = [[{}, {}, {}],
                        [{}, {}, {}]]
        self.user_label_dir = join(self.base_dir, conf['user_label'])
        self.feature_idx_gen = FeatureIdxGen(conf)
        self.user_label = UserLabel()
        self.app_usage = AppUsage()
        self.user_id_dev_info = {}

        self.gender_train_size = conf['gender_train_size_each_category']
        self.age_train_size = conf['age_train_size_each_category']

        self.app_usage_dir = join(self.base_dir, conf['contest_dataset_app_usage'])
        # print self.app_usage_dir

        # user_id -> info
        self.user_info = {}

        self.all_user_id_idx = {}

        self.base_day = datetime.datetime.strptime('1970-01-01', '%Y-%m-%d')

    def load_feature_idx(self):
        self.feature_idx_gen.add_appid_feature_idx()
        self.feature_idx_gen.add_query_feature_idx()
        self.feature_idx_gen.add_url_feature_idx()

    def user_id_in_gender_train_user_id_list(self, user_id):
        return user_id in self.train_gender_feature

    def user_id_in_gender_eval_user_id_list(self, user_id):
        return user_id in self.eval_gender_feature

    def user_id_in_gender_submit_user_id_list(self, user_id):
        return user_id in self.submit_gender_feature

    def user_id_in_age_train_user_id_list(self, user_id):
        return user_id in self.train_age_feature

    def app_id_in_train_app_id_list(self, app_id):
        if app_id in self.feature_idx_gen.appid_idx:
            return True
        return False

    def date_from_str(self, s):
        return (datetime.datetime.strptime(s, '%Y-%m-%d') - self.base_day).days

    def add_to_feature(self, feature_dict, user_id, feature_id, value, time):
        if user_id not in feature_dict:
            feature_dict[user_id] = {}
        time = self.date_from_str(time)
        if feature_id not in feature_dict[user_id]:
            feature_dict[user_id][feature_id] = [0.0, 0.0, time, time]
        t = feature_dict[user_id][feature_id]
        t[0] += value
        t[1] = min(t[1], time)
        t[2] = max(t[2], time)

    def update_min_time(self, m, x, y, t):
        tmp = m[x][y]
        if tmp == 0:
            m[x][y] = t
        else:
            m[x][y] = min(tmp, t)

    def update_max_time(self, m, x, y, t):
        tmp = m[x][y]
        if tmp == 0:
            m[x][y] = t
        else:
            m[x][y] = max(tmp, t)

    def write_feature_app_usage(self):
        for i in self.output_feature_dir:
            for j in i:
                if not os.path.exists(j):
                    os.makedirs(j)

        all_user_num = len(self.all_user_id_idx)
        all_feature_num = self.feature_idx_gen.feature_counter + 2  # 2 -> device info
        user_id_feature_id = zeros((all_user_num, all_feature_num), dtype=numpy.float64)
        user_id_feature_id_start_time = zeros((all_user_num, all_feature_num), dtype=numpy.int64)
        user_id_feature_id_end_time = zeros((all_user_num, all_feature_num), dtype=numpy.int64)
        # load from app usage
        line_cnt = 0
        for file in os.listdir(self.app_usage_dir):
            print self.app_usage_dir, file
            with open(join(self.app_usage_dir, file), 'r') as in_f:
                for line in in_f:
                    line_cnt += 1
                    if (line_cnt % 100000000) == 0:
                        print line_cnt
                    user_id, app_id, count, duration, time = line.strip().split()

                    if app_id in self.feature_idx_gen.appid_idx:
                        time = self.date_from_str(time)
                        count = float(count)
                        duration = float(duration)

                        user_id_idx = self.all_user_id_idx[user_id]
                        app_id_idx = self.feature_idx_gen.appid_idx[app_id]

                        user_id_feature_id[user_id_idx][app_id_idx] += duration
                        user_id_feature_id[user_id_idx][app_id_idx + 1] += count
                        self.update_min_time(user_id_feature_id_start_time, user_id_idx, app_id_idx, time)
                        self.update_max_time(user_id_feature_id_end_time, user_id_idx, app_id_idx, time)

        line_cnt = 0
        for file in os.listdir(self.query_dir):
            print self.query_dir, file
            with open(join(self.query_dir, file), 'r') as in_f:
                for line in in_f:
                    line_cnt += 1
                    if (line_cnt % 100000000) == 0:
                        print line_cnt
                    user_id, query_id, time = line.strip().split()
                    if query_id in self.feature_idx_gen.query_idx:
                        user_id_idx = self.all_user_id_idx[user_id]
                        query_id_idx = self.feature_idx_gen.query_idx[query_id]
                        time = self.date_from_str(time)
                        user_id_feature_id[user_id_idx][query_id_idx] += 1.0
                        self.update_min_time(user_id_feature_id_start_time, user_id_idx, query_id_idx, time)
                        self.update_max_time(user_id_feature_id_end_time, user_id_idx, query_id_idx, time)

        line_cnt = 0
        for file in os.listdir(self.url_dir):
            print self.url_dir, file
            with open(join(self.url_dir, file), 'r') as in_f:
                for line in in_f:
                    line_cnt += 1
                    if (line_cnt % 100000000) == 0:
                        print line_cnt
                    user_id, url_id, time = line.strip().split()
                    if url_id in self.feature_idx_gen.url_idx:
                        time = self.date_from_str(time)
                        user_id_idx = self.all_user_id_idx[user_id]
                        url_id_idx = self.feature_idx_gen.url_idx[url_id]
                        user_id_feature_id[user_id_idx][url_id_idx] += 1.0
                        self.update_min_time(user_id_feature_id_start_time, user_id_idx, url_id_idx, time)
                        self.update_max_time(user_id_feature_id_end_time, user_id_idx, url_id_idx, time)

        print 'start time'
        # print user_id_feature_id_start_time
        # print 'end time'
        # print user_id_feature_id_end_time
        # val / time_diff
        for i in xrange(0, all_user_num):
            for j in xrange(0, all_feature_num):
                e = user_id_feature_id_end_time[i][j]
                if e > 0:
                    s = user_id_feature_id_start_time[i][j]
                    user_id_feature_id /= (e - s + 1)
        user_id_feature_id_start_time = None
        user_id_feature_id_end_time = None

        print 'avg'
        # print user_id_feature_id

        user_id_feature_id = preprocessing.scale(user_id_feature_id, copy=False)
        print 'norm'
        # print user_id_feature_id

        device_brand_idx = {}
        device_brand_idx_cnt = 0
        device_brand_col = self.feature_idx_gen.feature_counter
        device_model_idx = {}
        device_model_idx_cnt = 0
        device_model_col = self.feature_idx_gen.feature_counter + 1
        print device_brand_col, device_model_col, "########"
        for user_id in self.user_id_dev_info.keys():
            device_brand, device_model = self.user_id_dev_info[user_id]
            if device_brand not in device_brand_idx:
                device_brand_idx[device_brand] = device_brand_idx_cnt
                device_brand_idx_cnt += 1
            if device_model not in device_model_idx:
                device_model_idx[device_model] = device_model_idx_cnt
                device_model_idx_cnt += 1
            user_id_idx = self.all_user_id_idx[user_id]
            # print "###", device_brand_idx[device_brand]
            # print "@@@", device_model_idx[device_model]
            user_id_feature_id[user_id_idx][device_brand_col] = device_brand_idx[device_brand]
            user_id_feature_id[user_id_idx][device_model_col] = device_model_idx[device_model]

        print 'device'
        # print user_id_feature_id
        feature_files = []
        tag_fiels = []
        for i in self.output_feature_dir:
            ff = []
            tf = []
            for j in i:
                ff.append(open(join(j, 'feature'), 'w'))
                tf.append(open(join(j, 'tag'), 'w'))
            feature_files.append(ff)
            tag_fiels.append(tf)

        # write feature file
        user_tag_getter = [self.user_label.get_user_gender, self.user_label.get_user_age]
        for user_id in self.all_user_id_idx.keys():
            user_id_idx = self.all_user_id_idx[user_id]
            for a in [0, 1]:
                for b in [0, 1, 2]:
                    if user_id in self.feature[a][b]:
                        feature_files[a][b].write('\t'.join([str(x) for x in user_id_feature_id[user_id_idx]]) + '\n')
                        # no tag for submit
                        if b != 2:
                            tag_fiels[a][b].write(str(user_tag_getter[a](user_id)) + '\n')
                        else:
                            tag_fiels[a][b].write(str(user_id) + '\n')
        for i in feature_files:
            for j in i:
                j.close()
        for i in tag_fiels:
            for j in i:
                j.close()

    def gen_train_eval_user_list(self):
        all_gender_feature = [[]]
        for i in xrange(1, 3):
            all_gender_feature.append([])
        all_age_feature = [[]]
        for i in xrange(1, 8):
            all_age_feature.append([])
        for user_id in self.user_label.user_info.keys():
            gender = self.user_label.get_user_gender(user_id)
            age = self.user_label.get_user_age(user_id)

            all_gender_feature[gender].append(user_id)
            all_age_feature[age].append(user_id)

        '''
        # shuffle
        for i in [all_age_feature, all_gender_feature]:
            l_i = len(i)
            for j in xrange(0, l_i):
                len_j = len(i[j])
                for k in xrange(0, len_j):
                    nk = random.randint(k, len_j - 1)
                    if k == nk:
                        continue
                    tmp = i[j][k]
                    i[j][k] = i[j][nk]
                    i[j][nk] = tmp
        '''

        train_gender_feature = {}
        eval_gender_feature = {}
        train_age_feature = {}
        eval_age_feature = {}
        # insert train and eval feature dict
        for j in xrange(1, 3):
            l_j = len(all_gender_feature[j])
            for k in xrange(0, l_j):
                u_id = all_gender_feature[j][k]
                if k < self.gender_train_size:
                    train_gender_feature[u_id] = j
                else:
                    eval_gender_feature[u_id] = j
        for i in xrange(1, 8):
            l_j = len(all_age_feature[i])
            for j in xrange(0, l_j):
                u_id = all_age_feature[i][j]
                if j < self.age_train_size:
                    train_age_feature[u_id] = i
                else:
                    eval_age_feature[u_id] = i

        submit_gender_feature = {}
        submit_age_feature = {}

        for i in os.listdir(self.test_data_dir):
            with open(join(self.test_data_dir, i), 'r') as f:
                for line in f:
                    user_id = line.strip()
                    submit_gender_feature[user_id] = 0
                    submit_age_feature[user_id] = 0

        self.feature = [
            [train_gender_feature, eval_gender_feature, submit_gender_feature],
            [train_age_feature, eval_age_feature, submit_age_feature]]

        all_user_id_cnt = 0
        for i in self.feature:
            for j in i:
                for k in j.keys():
                    if k not in self.all_user_id_idx:
                        self.all_user_id_idx[k] = all_user_id_cnt
                        all_user_id_cnt += 1
        for i in os.listdir(self.device_info_dir):
            with open(join(self.device_info_dir, i), 'r') as f:
                for line in f:
                    user_id, dev_brand, dev_model = line.strip().split()
                    self.user_id_dev_info[user_id] = (dev_brand, dev_model)
                    if user_id not in self.all_user_id_idx:
                        self.all_user_id_idx[user_id] = all_user_id_cnt
                        all_user_id_cnt += 1

                        # print 'all_user_id_idx:'
                        # print self.all_user_id_idx
                        # print 'feature:'
                        # for i in self.feature:
                        #            print i

    def load_all_labeled_user(self):
        self.user_label.load_data_from_base_dir(self.user_label_dir)


if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        gender_train_size = conf['gender_train_size_each_category']
        age_train_size = conf['age_train_size_each_category']
        test_data_dir = join(base_dir, conf['test_data_dir'])
        full_feature_dir = conf['output_feature_dir']
        output_feature_base = conf['classified_feature_dir']
        gender_train_output_feature_dir = join(output_feature_base, conf['gender_train_output_feature_dir'])
        gender_eval_output_feature_dir = join(output_feature_base, conf['gender_eval_output_feature_dir'])
        gender_submit_output_feature_dir = join(output_feature_base, conf['gender_submit_output_feature_dir'])

        age_train_output_feature_dir = join(output_feature_base, conf['age_train_output_feature_dir'])
        age_eval_output_feature_dir = join(output_feature_base, conf['age_eval_output_feature_dir'])
        age_submit_output_feature_dir = join(output_feature_base, conf['age_submit_output_feature_dir'])

        output_feature_dir = [[gender_train_output_feature_dir, gender_eval_output_feature_dir,
                               gender_submit_output_feature_dir],
                              [age_train_output_feature_dir, age_eval_output_feature_dir,
                               age_submit_output_feature_dir]]

        feature_files = []
        tag_files = []
        for i in output_feature_dir:
            ff = []
            tf = []
            for j in i:
                if not os.path.exists(j):
                    os.makedirs(j)
                ff.append(open(join(j, 'feature'), 'w'))
                tf.append(open(join(j, 'tag'), 'w'))
            feature_files.append(ff)
            tag_files.append(tf)

        user_label = UserLabel()
        user_label.load_data_from_base_dir(join(base_dir, conf['user_label']))
        all_gender_feature = [[]]
        for i in xrange(1, 3):
            all_gender_feature.append([])
        all_age_feature = [[]]
        for i in xrange(1, 8):
            all_age_feature.append([])
        for user_id in user_label.user_info.keys():
            gender = user_label.get_user_gender(user_id)
            age = user_label.get_user_age(user_id)

            all_gender_feature[gender].append(user_id)
            all_age_feature[age].append(user_id)

        # shuffle
        for i in [all_age_feature, all_gender_feature]:
            l_i = len(i)
            for j in xrange(0, l_i):
                len_j = len(i[j])
                for k in xrange(0, len_j):
                    nk = random.randint(k, len_j - 1)
                    if k == nk:
                        continue
                    tmp = i[j][k]
                    i[j][k] = i[j][nk]
                    i[j][nk] = tmp

        train_gender_feature = {}
        eval_gender_feature = {}
        train_age_feature = {}
        eval_age_feature = {}
        # insert train and eval feature dict
        for j in xrange(1, 3):
            l_j = len(all_gender_feature[j])
            for k in xrange(0, l_j):
                u_id = all_gender_feature[j][k]
                if k < gender_train_size:
                    train_gender_feature[u_id] = j
                else:
                    eval_gender_feature[u_id] = j
        for i in xrange(1, 8):
            l_j = len(all_age_feature[i])
            for j in xrange(0, l_j):
                u_id = all_age_feature[i][j]
                if j < age_train_size:
                    train_age_feature[u_id] = i
                else:
                    eval_age_feature[u_id] = i

        submit_gender_feature = {}
        submit_age_feature = {}

        feature = [
            [train_gender_feature, eval_gender_feature, submit_gender_feature],
            [train_age_feature, eval_age_feature, submit_age_feature]]

        for i in os.listdir(test_data_dir):
            with open(join(test_data_dir, i), 'r') as f:
                for line in f:
                    user_id = line.strip()
                    submit_gender_feature[user_id] = 0
                    submit_age_feature[user_id] = 0

        user_tag_getter = [user_label.get_user_gender, user_label.get_user_age]
        print full_feature_dir
        f = [open(join(full_feature_dir, x), 'r') for x in
             ['app_usage_all_feature', 'url_all_feature', 'query_all_feature', 'all_dev_feature']]
        while True:
            l = f[0].readline()
            if l == '':
                break
            l = l.strip()
            fields1 = l.split()
            fields2 = f[1].readline().strip().split()
            fields3 = f[2].readline().strip().split()
            fields4 = f[3].readline().strip().split()

            user_id = fields1[0]
            assert user_id == fields2[0]
            assert user_id == fields2[0]
            assert user_id == fields3[0]

            line = l + '\t' + '\t'.join(fields2[1:]) + '\t' + '\t'.join(fields3[1:]) + '\t' + '\t'.join(fields4[1:]) + '\n'
            for a in [0, 1]:
                for b in [0, 1, 2]:
                    cnt = 0
                    if user_id in feature[a][b]:
                        cnt += 1
                        feature_files[a][b].write(line)
                        if b != 2:
                            tag_files[a][b].write(str(user_tag_getter[a](user_id)) + '\n')
                        else:
                            tag_files[a][b].write(str(user_id) + '\n')
                        assert cnt == 1

        for a in [0, 1]:
            for b in [0, 1, 2]:
                feature_files[a][b].close()
                tag_files[a][b].close()
