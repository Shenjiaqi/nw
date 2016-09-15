import json
import os
import unittest
from os.path import join

from feature.HashDatasetAppUsage import HashDatasetAppUsage


class TestHashUserLabel(unittest.TestCase):
    def setUp(self):
        with open('data.json', 'r') as f:
            self.conf = json.load(f)

    def load_all_files(self, file_dir):
        recs = {}
        for file in os.listdir(file_dir):
            with open(join(file_dir, file)) as f:
                for line in f.readlines():
                    if line not in recs:
                        recs[line] = 0
                    recs[line] += 1
        return recs

    def test_hash_user_label(self):
        hasher = HashDatasetAppUsage()
        data_base_dir = self.conf['base_dir']
        source_dir = join(data_base_dir, self.conf['origin_dataset_app_usage'])
        target_dir = join(data_base_dir, self.conf['target_dataset_app_usage'])
        hasher.doHash(source_dir,
                      target_dir,
                      self.conf['hash_dataset_label_part_size'])
        source_data = self.load_all_files(source_dir)
        target_data = self.load_all_files(target_dir)
        for i in source_data.keys():
            self.assertTrue(i in target_data)
            self.assertEqual(source_data[i], target_data[i])
