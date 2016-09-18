import random
from os import listdir
from os.path import join

from scipy.sparse import csr_matrix


class FeatureLoader:
    def __init__(self):
        pass

    def load_age_feature(self, feature_dir):
        return self.load_feature(feature_dir, 'age')

    def load_gender_feature(self, feature_dir):
        return self.load_feature(feature_dir, 'gender')

    def load_feature(self, feature_dir, category):
        print feature_dir, category
        feature_folders = [f for f in sorted(listdir(join(feature_dir, category)))]
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
                            record = [float(x) for x in line.split(',')]
                            col_cnt = 0
                            for r in record:
                                if r > 1e-8:
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
