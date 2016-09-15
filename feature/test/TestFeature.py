import json
import unittest
from os.path import join

from feature.Feature import Feature
from feature.Normalizer import Normalizer


class TestFeature(unittest.TestCase):
    def setUp(self):
        with open('data.json') as f:
            conf = json.load(f)
            self.feature_dir = conf['feature_dir']
            self.normalize_dir = conf['norm_feature_dir']
            self.feature = Feature(conf=conf)
            self.feature.load_data()
            self.base_dir = conf['base_dir']

    def test_generate_user_feature_by_topk_open_appid(self):
        self.feature.generate_user_feature_by_topk_open_appid(2)
        normalizer = Normalizer()
        source_dir = join(self.base_dir, self.feature_dir)
        target_dir = join(self.base_dir, self.normalize_dir)
        normalizer.normalize_age(source_dir, target_dir)
        normalizer.normalize_gender(source_dir, target_dir)

        '''
        self.assertTrue()
        # only user3 and user4 used app3
        self.assertEqual(2, len(result))
        self.assertEqual(4, len(result[0]))
        # user3: 1 2
        # user4: 2 1
        # user3 and user4 use app3 equally
        self.assertTrue([1, 2, 0.5, 0.5] in result)
        self.assertTrue([2, 1, 0.5, 0.5] in result)

        result = self.feature.generate_user_feature_by_topk_open_appid(2)
        self.assertEqual(2, len(result))
        #
        self.assertTrue([2, 1, 0.5, 0.5, 1.0, 1.0] in result or
                        [2, 1, 1.0, 1.0, 0.5, 0.5] in result)
                        '''

if __name__ == '__main__':
    unittest.main()