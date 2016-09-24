import json
import os
from os.path import join


class FeatureIdxGen:
    def __init__(self, conf):
        self.base_dir = conf['base_dir']
        self.uv_dir = join(self.base_dir, conf['uv_dir'])
        self.uv_top100_appid_dir = join(self.uv_dir, conf['usage_top100_appid'])
        self.uv_install_top100_appid_dir = join(self.uv_dir, conf['install_top100_appid'])
        self.uv_dir = join(self.base_dir, conf['uv_dir'])
        self.uv_url_top100_dir = join(self.uv_dir, conf['url_top100'])
        self.uv_query_top100_dir = join(self.uv_dir, conf['query_top100'])
        self.user_label_dir = join(self.base_dir, conf['user_label'])
        self.query_idx = {}
        self.url_idx = {}
        self.appid_idx = {}
        self.feature_counter = 0
        self.user_counter = 0

    def add_appid_feature_idx(self):
        for i in [self.uv_top100_appid_dir, self.uv_install_top100_appid_dir]:
            for j in os.listdir(i):
                with open(join(i, j), 'r') as f:
                    for l in f:
                        app_id = l.strip()
                        if app_id not in self.appid_idx:
                            self.appid_idx[app_id] = self.feature_counter
                            self.feature_counter += 2

    def add_query_feature_idx(self):
        for i in os.listdir(self.uv_query_top100_dir):
            with open(join(self.uv_query_top100_dir, i), 'r') as f:
                for l in f:
                    query_id = l.strip()
                    if query_id not in self.query_idx:
                        self.query_idx[query_id] = self.feature_counter
                        self.feature_counter += 1

    def add_url_feature_idx(self):
        for i in os.listdir(self.uv_url_top100_dir):
            with open(join(self.uv_url_top100_dir, i), 'r') as f:
                for l in f:
                    url_id = l.strip()
                    if url_id not in self.url_idx:
                        self.url_idx[url_id] = self.feature_counter
                        self.feature_counter += 1

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        feature_idx_gen = FeatureIdxGen(conf)
        feature_idx_gen.add_query_feature_idx()
        feature_idx_gen.add_url_feature_idx()
        feature_idx_gen.add_appid_feature_idx()

        print feature_idx_gen.feature_counter
        print feature_idx_gen.appid_idx
        print feature_idx_gen.url_idx
        print feature_idx_gen.query_idx
