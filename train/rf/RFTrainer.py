import json
import os
from os.path import join
import sys

# from train import FeatureLoader
from numpy import zeros

sys.path.append("..")


import pickle
from sklearn.ensemble import RandomForestClassifier


class RFTrainer:
    def __init__(self):
        self.age_random_forest = RandomForestClassifier(n_jobs=10, n_estimators=100, min_samples_split=5, max_depth=10,
                                                        min_samples_leaf=10)
        self.gender_random_forest = RandomForestClassifier(n_jobs=10, n_estimators=100, min_samples_split=5,
                                                           max_depth=10, min_samples_leaf=10)
        #self.feature_loader = FeatureLoader()
        self.age_feature = None
        self.gender_feature = None

    def load_age_feature(self, base_dir, feature_dir):
        self.age_feature, self.age_target = self.feature_loader.load_data_less_than_n(base_dir, feature_dir, 3000000,
                                                                                      'age')
        # self.age_feature, self.age_target = \
        #        self.feature_loader.load_age_feature(feature_dir=feature_dir)

    def load_gender_feature(self, base_dir, feature_dir):
        self.gender_feature, self.gender_target = self.feature_loader.load_data_less_than_n(base_dir, feature_dir,
                                                                                            3000000,
                                                                                            'gender')
        # self.gender_feature, self.gender_target = \
        # self.feature_loader.load_gender_feature(feature_dir=feature_dir)

    def handle_age_feature(self):
        self.age_random_forest

    def train_age(self):
        self.age_random_forest.fit(self.age_feature, self.age_target)

    def train_gender(self):
        self.gender_random_forest.fit(self.gender_feature, self.gender_target)

    def predict_age(self, feature):
        return self.age_random_forest.predict_proba(feature)

    def predict_gender_proba(self, feature):
        return self.gender_random_forest.predict_proba(feature)

    def predict_gender(self, feature):
        return self.gender_random_forest.predict(feature)

    def save_age_model(self, model_dir):
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
        with open(join(model_dir, 'age_model'), 'w') as f:
            pickle.dump(self.age_random_forest, f)

    def save_gender_model(self, model_dir):
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
        with open(join(model_dir, 'gender_model'), 'w') as f:
            pickle.dump(self.gender_random_forest, f)

    def save_model(self, model_dir):
        self.save_age_model(model_dir)
        self.save_gender_model(model_dir)

    def load_age_model(self, model_dir):
        with open(join(model_dir, 'age_model'), 'r') as f:
            self.age_random_forest = pickle.load(f)

    def load_gender_model(self, model_dir):
        with open(join(model_dir, 'gender_model'), 'r') as f:
            self.gender_random_forest = pickle.load(f)

    def load_model(self, model_dir):
        self.load_age_model(model_dir)
        self.load_gender_model(model_dir)

import scipy as sp
def logloss(act, pred):
    epsilon = 1e-15
    pred = sp.maximum(epsilon, pred)
    pred = sp.minimum(1-epsilon, pred)
    ll = sum(act*sp.log(pred) + sp.subtract(1,act)*sp.log(sp.subtract(1,pred)))
    ll = ll * -1.0/len(act)
    return ll

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        feature_dir = join(base_dir, conf['feature_dir'])
        model_dir = join(base_dir, conf['model_dir'])

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

        final_output_dir = conf['final_eval_dir']
        feature_files = []
        tags_files = []
        for i in output_feature_dir:
            f = []
            t = []
            for j in i:
                f.append(open(join(j, 'feature'), 'r'))
                t.append(open(join(j, 'tag'), 'r'))

            feature_files.append(f)
            tags_files.append(t)


        for estimators in [100]:
            for samples_split in [5]:
                for max_depth in [10]:
                    for samples_leaf in [10]:
                        for i in feature_files:
                            for j in i:
                                j.seek(0)
                        for i in tags_files:
                            for j in i:
                                j.seek(0)
                        for i_file in [0, 1]:
                            out_dir = join(final_output_dir, 'est_n' + str(estimators) +
                                           'split' + str(samples_split) +
                                           'depth' + str(max_depth) +
                                           'leaf' + str(samples_leaf))
                            output_dir = join(out_dir, 'gender' if i_file == 0 else 'age')
                            if not os.path.exists(output_dir):
                                os.makedirs(output_dir)
                            eval_dir = join(output_dir, 'eval')
                            if not os.path.exists(eval_dir):
                                os.makedirs(eval_dir)
                            submit_file = join(output_dir, 'submit')
                            # train feature

                            train_feature = None
                            tag = []
                            feature_line_cnt = 0
                            field_width = 0
                            for l in feature_files[i_file][0]:
                                feature_line_cnt += 1
                                if feature_line_cnt % 1000000 == 0:
                                    print "feature line: " + str(feature_line_cnt)
                                fields = l.strip().split()
                                if train_feature == None:
                                    field_width = len(fields) - 1
                                    train_feature = zeros((6000001, field_width))
                                for i in xrange(0, field_width):
                                    train_feature[feature_line_cnt - 1][i] = float(fields[i+1])
                            train_feature.resize((feature_line_cnt, field_width))
                            print feature_line_cnt, field_width
                            # train tag
                            for l in tags_files[i_file][0]:
                                tag.append(int(l.strip()))

                            rf = RandomForestClassifier(n_jobs=20, n_estimators=estimators,
                                                        min_samples_split=samples_split,
                                                        max_depth=max_depth,
                                                        min_samples_leaf=samples_leaf)

                            rf.fit(train_feature, tag)
                            train_feature = None
                            tag = []

                            # eval
                            right_file = open(join(eval_dir, 'r'), 'w')
                            wrong_file = []
                            loglosssum = 0.0
                            num = 0.0
                            for i in xrange(0, 8):
                                wrong_file.append(open(join(eval_dir, 'f_' + str(i)), 'w'))
                            eval_cnt = 0
                            for l in feature_files[i_file][1]:
                                eval_cnt += 1
                                if eval_cnt > 1000000:
                                    break
                                if eval_cnt % 10000 == 0:
                                    print "eval: " + str(eval_cnt)
                                fields = l.strip().split()
                                user_id = fields[0]
                                feature = [float(x) for x in fields[1:]]
                                tag_i = int(tags_files[i_file][1].readline().strip())
                                eval_result = rf.predict_proba([feature])
                                j = 0
                                for k in xrange(0, len(eval_result[0])):
                                    if eval_result[0][k] > eval_result[0][j]:
                                        j = k
                                j += 1
                                result_str = user_id + '\t' + '\t'.join([str(x) for x in eval_result]) + '\n'
                                act = [0 for x in xrange(0, len(eval_result[0]))]
                                act[tag_i - 1] = 1
                                loglosssum += logloss(act, eval_result[0])
                                num += 1.0
                                if j != tag_i:
                                    wrong_file[j].write(result_str)
                                else:
                                    right_file.write(result_str)
                            avg_log_loss = loglosssum / (num + 0.00000001)
                            with open(join(eval_dir, 'log_logss'), 'w') as out_f:
                                out_f.write(str(avg_log_loss))
                            for i in wrong_file:
                                i.close()
                            right_file.close()
                            with open(submit_file, 'w') as out_f:
                                for l in feature_files[i_file][2]:
                                    fields = l.strip().split()
                                    result = rf.predict([fields[1:]])
                                    out_f.write(fields[0] + ',' + ','.join([str(x) for x in result]) + '\n')

                            with open(join(eval_dir, 'model'), 'w') as f:
                                pickle.dump(rf, f)
