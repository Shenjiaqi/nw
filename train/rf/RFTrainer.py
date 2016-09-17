import json
import os
from os.path import join
import sys

sys.path.append("..")

from FeatureLoader import FeatureLoader

import pickle
from sklearn.ensemble import RandomForestClassifier


class RFTrainer:
    def __init__(self):
        self.age_random_forest = RandomForestClassifier()
        self.gender_random_forest = RandomForestClassifier()
        self.feature_loader = FeatureLoader()

    def handle_age_feature(self):
        self.age_random_forest
    def train_age(self):
        self.feature_loader.scan_feature()
        self.age_random_forest.fit(self.age_feature, self.age_target)

    def train_gender(self):
        self.gender_random_forest.fit(self.gender_feature, self.gender_target)

    def predict_age(self, feature):
        return self.age_random_forest.predict_proba(feature)

    def predict_gender(self, feature):
        return self.gender_random_forest.predict_proba(feature)

    def save_age_model(self, model_dir):
        s = pickle.dumps(self.age_random_forest)
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
        with open(join(model_dir, 'age_model'), 'w') as f:
            f.write(s)

    def save_gender_model(self, model_dir):
        s = pickle.dumps(self.gender_random_forest)
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
        with open(join(model_dir, 'gender_model'), 'w') as f:
            f.write(s)

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

zero_cnt = 0
all_cnt = 0
def handler_feature(tag, feature):
    global zero_cnt
    global all_cnt
    all_cnt += 1
    zero_cnt += 1
    for i in feature:
        if i > 1e-8:
            zero_cnt -= 1
            break

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        feature_dir = join(base_dir, conf['norm_feature_dir'])

        feature_loader = FeatureLoader()
        feature_loader.scan_feature(feature_dir, 'age', handle_feature=handler_feature)
        global zero_cnt, all_cnt
        print zero_cnt, all_cnt, float(zero_cnt) / all_cnt
        '''
        rf_trainer = RFTrainer()
        rf_trainer.load_feature(feature_dir=feature_dir)
        rf_trainer.train_age()
        rf_trainer.train_gender()
        rf_trainer.save_model(join(base_dir, conf['model_dir']))
        '''

