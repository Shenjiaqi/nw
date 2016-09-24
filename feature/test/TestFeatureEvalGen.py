import json
import unittest

from feature.FeatureEvalGen import FeatureEvalGen
from feature.UserLabel import UserLabel


class TestFeatureEvalGen(unittest.TestCase):
    def setUp(self):
        with open('data.json', 'r') as f:
            conf = json.load(f)
            self.feature_eval_gen = FeatureEvalGen(conf)

    def test_load_feature_idx(self):
        self.feature_eval_gen.load_feature_idx()
        self.assertEqual(5, len(self.feature_eval_gen.feature_idx_gen.appid_idx))
        self.assertEqual(4, len(self.feature_eval_gen.feature_idx_gen.url_idx))
        self.assertEqual(4, len(self.feature_eval_gen.feature_idx_gen.query_idx))

    def test_load_all_labeled_user(self):
        self.feature_eval_gen.load_all_labeled_user()
        self.assertEqual(5, len(self.feature_eval_gen.user_label.user_info))
        self.assertTrue('user1' in self.feature_eval_gen.user_label.user_info.keys())

    def test_gen_train_eval_user_list(self):
        self.feature_eval_gen.load_all_labeled_user()
        self.feature_eval_gen.gen_train_eval_user_list()
        self.assertEqual(2, len(self.feature_eval_gen.train_gender_feature))
        self.assertEqual(2, len(self.feature_eval_gen.eval_gender_feature))

        self.assertEqual(2, len(self.feature_eval_gen.train_gender_feature[1]) +
                         len(self.feature_eval_gen.train_gender_feature[2]))
        self.assertEqual(3, len(self.feature_eval_gen.eval_gender_feature[1]) +
                         len(self.feature_eval_gen.eval_age_feature[2]))

        for c in xrange(1, 3):
            for i in self.feature_eval_gen.train_gender_feature[c]:
                self.assertTrue(i not in self.feature_eval_gen.eval_gender_feature[c])

        self.assertEqual(7, len(self.feature_eval_gen.train_age_feature))
        self.assertEqual(7, len(self.feature_eval_gen.eval_age_feature))
        all_age_train_num = 0
        all_age_eval_num = 0
        for i in self.feature_eval_gen.train_age_feature.keys():
            all_age_train_num += len(self.feature_eval_gen.train_age_feature[i])
            all_age_eval_num += len(self.feature_eval_gen.eval_age_feature[i])
        self.assertEqual(4, all_age_train_num)
        self.assertEqual(1, all_age_eval_num)

    def test_write_user_app_usage_feature(self):
        #self.feature_eval_gen.load_all_labeled_user()
        user_label = UserLabel()
        for i in xrange(1, 8):
            user_label.add_user('user' + str(i), 2 - (i & 1), i)
        self.feature_eval_gen.user_label = user_label
        self.feature_eval_gen.train_gender_feature = {1: ['user1', 'user3'], 2: ['user2']}
        self.feature_eval_gen.train_age_feature = {1: ['user1'],
                                                   2: ['user2'],
                                                   3: ['user3'],
                                                   4: ['user4'],
                                                   5:[], 6:[], 7:[]}

        self.feature_eval_gen.load_feature_idx()
        self.feature_eval_gen.write_gender_feature_app_usage()
        self.feature_eval_gen.write_age_feature_app_usage()
