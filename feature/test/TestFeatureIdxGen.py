import json
import unittest

from feature.FearureIdxGen import FeatureIdxGen


class TestFeatureIdxGen(unittest.TestCase):
    def setUp(self):
        with open('data.json', 'r') as f:
            conf = json.load(f)
            self.feature_idx_gen = FeatureIdxGen(conf)

    def test_appid_feature_gen(self):
        self.feature_idx_gen.add_appid_feature_idx()
        self.assertEqual(5, self.feature_idx_gen.feature_counter)
        self.assertEqual(5, len(self.feature_idx_gen.app_feature_idx))
        vals = []
        for v in self.feature_idx_gen.app_feature_idx.values():
            vals.append(v)
        sorted_vals = sorted(vals)
        self.assertEqual([i for i in xrange(0, 5)], sorted_vals)

    def test_query_feature_gen(self):
        self.feature_idx_gen.add_query_feature_idx()
        self.assertEqual(4, len(self.feature_idx_gen.query_idx))
        self.feature_idx_gen.add_appid_feature_idx()
        self.assertEqual(4 + 5, self.feature_idx_gen.feature_counter)

    def test_url_feature_gen(self):
        self.feature_idx_gen.add_url_feature_idx()
        self.feature_idx_gen.add_query_feature_idx()
        self.feature_idx_gen.add_appid_feature_idx()
        self.assertEqual(4 + 5 + 4, self.feature_idx_gen.feature_counter)
        vals = []
        for i in [self.feature_idx_gen.app_feature_idx, self.feature_idx_gen.url_idx, self.feature_idx_gen.query_idx]:
            for j in i.values():
                vals.append(j)
        sorted_values = sorted(vals)
        self.assertEqual([i for i in xrange(0, self.feature_idx_gen.feature_counter)],
                         sorted_values)