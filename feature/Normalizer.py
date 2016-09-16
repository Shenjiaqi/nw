import json
import os
import shutil
from os.path import join


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
        for file in os.listdir(file_dir):
            file_path = join(file_dir, file)
            with open(file_path, 'r') as f:
                for line in f.readlines():
                    c = 0
                    for column in line.split(','):
                        if len(column_sum) <= c:
                            column_sum.append(0.0)
                        column_sum[c] += float(column)
                        c += 1
        for file in os.listdir(file_dir):
            file_path = join(file_dir, file)
            with open(join(target_dir, file), 'w') as tf:
                with open(file_path, 'r') as sf:
                    for line in sf.readlines():
                        target_line = []
                        c = 0
                        for column in line.split(','):
                            target_line.append(0 if column_sum[c] < 1e-9 else float(column) / column_sum[c])
                            c += 1
                        tf.write(','.join([str(x) for x in target_line]) + '\n')

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        normalize = Normalizer()
        base_dir = conf['base_dir']
        source_dir = join(base_dir, conf['feature_dir'])
        target_dir = join(base_dir, conf['norm_feature_dir'])
        normalize.normalize(file_dir=source_dir, target_dir=target_dir)
