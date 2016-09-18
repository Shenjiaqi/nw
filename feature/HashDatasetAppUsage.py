import json
import os
import shutil
from os.path import join


class HashDatasetAppUsage:
    def __init__(self):
        pass

    #
    def doHash(self, dataset_app_usage, target_hashed_label_dir, hashed_size):
        target_files = []
        if not os.path.exists(target_hashed_label_dir):
            os.makedirs(target_hashed_label_dir)

        for i in xrange(0, hashed_size):
            target_files.append(open(join(target_hashed_label_dir, str(i)), 'w', 128))

        cnt = 0
        for file_name in os.listdir(dataset_app_usage):
            with open(join(dataset_app_usage, file_name), 'r', 1024) as f:
                for l in f:
                    cnt += 1
                    if cnt % 1000 == 0:
                        print cnt
                    user_id, app_id, count, duration, date = l.split()
                    target_files[hash(user_id) % len(target_files)].write(l)

        for i in target_files:
            i.close()

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        data_base_dir = conf['base_dir']
        origin_dataset_label_dir = join(data_base_dir, conf['origin_dataset_app_usage'])
        target_dataset_label_dir = join(data_base_dir, conf['target_dataset_app_usage'])
        size = conf['hash_dataset_label_part_size']
        if os.path.exists(target_dataset_label_dir):
            shutil.rmtree(target_dataset_label_dir)
        hash_user_label = HashDatasetAppUsage()
        hash_user_label.doHash(dataset_app_usage=origin_dataset_label_dir,
                               target_hashed_label_dir=target_dataset_label_dir,
                               hashed_size=size)
