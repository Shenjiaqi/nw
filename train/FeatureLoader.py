from os import listdir
from os.path import join
from sklearn import datasets


class FeatureLoader:
    def __init__(self):
        pass

    def load_age_feature(self, feature_dir):
        return self.load_feature(feature_dir, 'age')

    def load_gender_feature(self, feature_dir):
        return self.load_feature(feature_dir, 'gender')

    def load_feature(self, feature_dir, category):
        feature_folders = [f for f in sorted(listdir(join(feature_dir, category)))]
        records = []
        tags = []
        for f in feature_folders:
            c = int(f)
            class_dir = join(feature_dir, category, f)
            for file in listdir(class_dir):
                with open(join(class_dir, file), 'r') as f:
                    for line in f.readlines():
                        record = [float(x) for x in line.split(',')]
                        records.append(record)
                        tags.append(c)
        return records, tags

    def scan_feature(self, feature_dir, category, handle_feature):
        feature_folders = [f for f in sorted(listdir(join(feature_dir, category)))]
        for f in feature_folders:
            c = int(f)
            class_dir = join(feature_dir, category, f)
            for file in listdir(class_dir):
                with open(join(class_dir, file), 'r') as f:
                    for line in f.readlines():
                        record = [float(x) for x in line.split(',')]
                        handle_feature(c, record)
