import json
import unittest

from feature.Feature import Feature


class TestFeature(unittest.TestCase):
    def setUp(self):
        self.feature = Feature()
        with open('data.json') as f:
            conf = json.load(f)
            self.feature.load_data(conf=conf)

    def test_generate_user_feature_by_topk_open_appid(self):
        result = self.feature.generate_user_feature_by_topk_open_appid(1)
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

if __name__ == '__main__':
    unittest.main()