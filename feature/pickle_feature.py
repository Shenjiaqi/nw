import json
import pickle
import os
from os.path import join

import numpy
from numpy import zeros


def get_pickle_file(file_path, line_limit):
    file_name = join(file_path, 'feature')
    user_list = []
    mtr = None
    line_cnt = 0
    width = 0
    if os.path.exists(file_name):
        with open(file_name, 'r') as in_f:
            with open(file_name + ".pickle", 'wb') as out_f:
                for line in in_f:
                    if line_cnt >= line_limit:
                        break
                    line_cnt += 1
                    fields = line.strip().split()
                    user_list.append(fields[0])
                    if mtr is None:
                        width = len(fields) - 1
                        mtr = zeros((6000000, width), dtype=numpy.float64)
                    for i in xrange(1, width):
                        mtr[line_cnt-1][i-1] = float(fields[i])
                mtr.resize((line_cnt, width))
                pickle.dump(mtr, out_f)
        with open(join(file_path, 'user_id.pickle'), 'wb') as out_f:
            pickle.dump(user_list, out_f)

    file_name = join(file_path, 'tag')
    tag_list = []
    if os.path.exists(file_name):
        with open(file_name, 'r') as in_f:
            with open(file_name + '.pickle', 'wb') as out_f:
                line_cnt = 0
                for line in in_f:
                    if line_cnt >= line_limit:
                        break
                    line_cnt += 1
                    tag_list.append(int(line.strip()))
                pickle.dump(tag_list, out_f)


if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        feature_dir = join(base_dir, conf['feature_dir'])

        output_feature_base = conf['classified_feature_dir']
        gender_train_output_feature_dir = join(output_feature_base, conf['gender_train_output_feature_dir'])
        gender_eval_output_feature_dir = join(output_feature_base, conf['gender_eval_output_feature_dir'])
        gender_submit_output_feature_dir = join(output_feature_base, conf['gender_submit_output_feature_dir'])

        age_train_output_feature_dir = join(output_feature_base, conf['age_train_output_feature_dir'])
        age_eval_output_feature_dir = join(output_feature_base, conf['age_eval_output_feature_dir'])
        age_submit_output_feature_dir = join(output_feature_base, conf['age_submit_output_feature_dir'])

        inf = 1e9
        eval_num = 100000
        for i in [(gender_train_output_feature_dir, inf),
                  (gender_eval_output_feature_dir, eval_num),
                  (gender_submit_output_feature_dir, inf),
                  (age_train_output_feature_dir, inf),
                  (age_eval_output_feature_dir, eval_num),
                  (age_submit_output_feature_dir, inf)]:
            get_pickle_file(i[0], i[1])