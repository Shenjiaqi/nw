import json
import os
import shutil
from os.path import join

import numpy
from numpy import zeros
from sklearn import preprocessing


class Normalizer:
    def __init__(self):
        pass

    def normalize_gender(self, file_dir, target_dir):
        return self.normalize(join(file_dir, 'gender'), join(target_dir, 'gender'))

    def normalize_age(self, file_dir, target_dir):
        return self.normalize(join(file_dir, 'age'), join(target_dir, 'age'))

    def normalize(self, file_dir, target_dir):
        # clear dir
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        os.makedirs(target_dir)

        column_sum = []
        for category_dir in os.listdir(file_dir):
            for file in os.listdir(join(file_dir, category_dir)):
                file_path = join(file_dir, category_dir, file)
                with open(file_path, 'r') as f:
                    for line in f:
                        c = 0
                        for column in line.split(','):
                            if len(column_sum) <= c:
                                column_sum.append(0.0)
                            column_sum[c] += float(column)
                            c += 1
        for category_dir in os.listdir(file_dir):
            for file in os.listdir(join(file_dir, category_dir)):
                file_path = join(file_dir, category_dir, file)
                target_category_dir = join(target_dir, category_dir)
                if not os.path.exists(target_category_dir):
                    os.makedirs(target_category_dir)
                with open(join(target_category_dir, file), 'w') as tf:
                    with open(file_path, 'r') as sf:
                        for line in sf:
                            target_line = []
                            c = 0
                            for column in line.split(','):
                                target_line.append(0 if column_sum[c] < 1e-9 else float(column) / column_sum[c])
                                c += 1
                            tf.write(','.join([str(x) for x in target_line]) + '\n')


if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        reduced_feature_dir = conf['reduced_dir']
        feature_output_dir = conf['output_feature_dir']
        if not os.path.exists(feature_output_dir):
            os.makedirs(feature_output_dir)
        user_id_map = {}
        user_id_map_size = 0
        feature_id_map = {}
        feature_id_map_size = 0
        device_id_map = {}
        device_id_map_size = 0
        width = 490
        all_raw_feature = zeros((12439410, width), dtype=numpy.float64)
        dev_raw_feature = zeros((all_raw_feature.shape[0], 2), numpy.float64)
        for i in os.listdir(reduced_feature_dir):
            print i
            if i.startswith('device_'):
                # device info do not need to be normalized
                with open(join(reduced_feature_dir, i), 'r') as in_f:
                    for line in in_f:
                        user_id, dev_feature_id, value = line.split()
                        if user_id not in user_id_map:
                            user_id_map[user_id] = user_id_map_size
                            user_id_map_size += 1
                        if dev_feature_id not in device_id_map:
                            device_id_map[dev_feature_id] = device_id_map_size
                            device_id_map_size += 1
                        user_id_idx = user_id_map[user_id]
                        dev_id_idx = device_id_map[dev_feature_id]
                        dev_raw_feature[user_id_idx][dev_id_idx] = float(value)
                continue
            with open(join(reduced_feature_dir, i), 'r') as in_f:
                for line in in_f:
                    user_id, feature_id, value = line.strip().split()
                    if user_id not in user_id_map:
                        user_id_map[user_id] = user_id_map_size
                        user_id_map_size += 1
                    if feature_id not in feature_id_map:
                        feature_id_map[feature_id] = feature_id_map_size
                        feature_id_map_size += 1
                    user_id_idx = user_id_map[user_id]
                    feature_id_idx = feature_id_map[feature_id]
                    all_raw_feature[user_id_idx][feature_id_idx] = float(value)
        all_raw_feature.resize((user_id_map_size, width))
        print all_raw_feature.shape
        preprocessing.scale(all_raw_feature, copy=False)

        with open(join(feature_output_dir, 'all_feature'), 'w') as out_f:
            for user_id in user_id_map.keys():
                user_id_idx = user_id_map[user_id]
                out_f.write(user_id + '\t' + '\t'.join(
                    [str(x) for x in all_raw_feature[user_id_idx][:feature_id_map_size]]) + '\t' + '\t'.join([str(x) for x in dev_raw_feature[user_id_idx]]) + '\n')
