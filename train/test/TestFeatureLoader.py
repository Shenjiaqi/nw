import json
import unittest

from train import FeatureLoader


class TestFeatureLoader(unittest.TestCase):
    def setUp(self):
        self.feature_loader = FeatureLoader.FeatureLoader()
        with open('data.json', 'r') as f:
            self.conf = json.load(f)

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

if __name__ == '__main__':
    unittest.main()