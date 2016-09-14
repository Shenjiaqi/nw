import json
from os.path import join

from sklearn.ensemble import RandomForestClassifier
from sklearn import datasets

from train import FeatureLoader


class RFTrainer:
    def __init__(self):
        self.age_random_forest = RandomForestClassifier()
        self.gender_random_forest = RandomForestClassifier()
        self.age_feature = None
        self.gender_feature = None
        self.feature_loader = FeatureLoader.FeatureLoader()

    def load_feature(self, feature_dir):
        self.age_feature, self.age_target = \
            self.feature_loader.load_age_feature(feature_dir=feature_dir)
        print self.age_feature
        self.gender_feature, self.age_target =\
            self.feature_loader.load_gender_feature(feature_dir=feature_dir)
        print self.age_feature

    def train_age(self):
        self.age_random_forest.fit(self.age_feature, self.age_target)

    def train_gender(self):
        self.gender_random_forest.fit(self.gender_feature, self.gender_target)

    def predict_age(self, feature):
        return self.age_random_forest.predict(feature)

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        feature_dir = conf['feature_dir']
        rf_trainer = RFTrainer()
        rf_trainer.load_feature(feature_dir=feature_dir)
        rf_trainer.train_age()