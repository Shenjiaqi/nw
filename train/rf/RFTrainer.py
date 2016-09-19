import json
import os
from os.path import join
import sys

#from train import FeatureLoader

sys.path.append("..")

from FeatureLoader import FeatureLoader

import pickle
from sklearn.ensemble import RandomForestClassifier


class RFTrainer:
    def __init__(self):
        self.age_random_forest = RandomForestClassifier(n_jobs=7)
        self.gender_random_forest = RandomForestClassifier(n_jobs=7)
        self.feature_loader = FeatureLoader()
        self.age_feature = None
        self.gender_feature = None

    def load_age_feature(self, base_dir):
        self.age_feature, self.age_target = self.feature_loader.load_age_data_less_than_n(self.base_dir, 1000000)
        #self.age_feature, self.age_target = \
        #        self.feature_loader.load_age_feature(feature_dir=feature_dir)

    def load_gender_feature(self, feature_dir):
        self.gender_feature, self.gender_target = \
            self.feature_loader.load_gender_feature(feature_dir=feature_dir)

    def handle_age_feature(self):
        self.age_random_forest

    def train_age(self):
        self.age_random_forest.fit(self.age_feature, self.age_target)

    def train_gender(self):
        self.gender_random_forest.fit(self.gender_feature, self.gender_target)

    def predict_age(self, feature):
        return self.age_random_forest.predict_proba(feature)

    def predict_gender(self, feature):
        return self.gender_random_forest.predict_proba(feature)

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

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        feature_dir = join(base_dir, conf['norm_feature_dir'])
        model_dir = join(base_dir, conf['model_dir'])

        rf_trainer = RFTrainer()
        print 'load age feature'
        rf_trainer.load_age_feature(base_dir=base_dir)
        print 'train age model'
        rf_trainer.train_age()
        print 'save age model'
        rf_trainer.save_age_model(model_dir)
        '''
        rf_trainer = RFTrainer()
        print 'load gender feature'
        rf_trainer.load_gender_feature(feature_dir=feature_dir)
        print 'train gender feature'
        rf_trainer.train_gender()
        print 'save gender feature'
        rf_trainer.save_gender_model(model_dir)'''
