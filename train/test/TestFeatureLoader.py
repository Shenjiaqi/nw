import json
import unittest

from train import FeatureLoader


class TestFeatureLoader(unittest.TestCase):
    def setUp(self):
        self.feature_loader = FeatureLoader()
        with open('data.json', 'r') as f:
            self.conf = json.load(f)
            self.base_dir = self.conf['base_dir']

    def test_load_age_feature(self):
        feature, classes = self.feature_loader.load_age_feature(self.conf['feature_dir'])
        self.assertEqual(2, len(feature))
        self.assertEqual(2, len(classes))
        self.assertEqual(3, len(feature[0]))

    def test_load_gender_feature(self):
        feature, classes = self.feature_loader.load_gender_feature(self.conf['feature_dir'])
        self.assertEqual(3, len(feature))
        self.assertEqual(3, len(classes))
        self.assertEqual(1, len(feature[0]))
        for i in xrange(len(feature)):
            self.assertEqual(feature[i][0], classes[i])

    def test_load_gender_data(self):
        m, t = self.feature_loader.load_data_less_than_n(self.base_dir, 2, 'age')

        print m, t
        m2, t2 = self.feature_loader.load_data_less_than_n(self.base_dir, 1, 'age')
        print m2, t2


if __name__ == '__main__':
    unittest.main()