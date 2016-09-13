import json
import unittest

from feature import AppUsage


class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.app_usage = AppUsage()
        with open('data.json', 'r') as f:
            conf = json.load(f)
            self.app_usage.load_data_from_base_dir(conf['base_dir'])

    def test_get_topk_open_appid(self):
        result = self.app_usage.get_topk_open_appid(2)
        self.assertEqual(2, len(result))
        self.assertTrue('app3' in result.keys())
        self.assertTrue('app4' in result.keys())

    def test_extract_record(self):
        recs = self.app_usage.extract_record(lambda user_id,
                                                    app_id,
                                                    count,
                                                    duration,
                                                    date: user_id == 'user1' or app_id == 'app2')
        self.assertEqual(3, len(recs))
        recs = self.app_usage.extract_record(lambda user_id,
                                                    app_id,
                                                    count,
                                                    duration,
                                                    date: user_id == 'user3' and app_id == 'app3')
        self.assertEqual(1, len(recs))
        self.assertEqual('user3', recs[0]['user_id'])
        self.assertEqual('app3', recs[0]['app_id'])
        self.assertEqual(3, recs[0]['count'])
        self.assertEqual(3, recs[0]['duration'])
        self.assertEqual('2016-08-03', recs[0]['date'])

if __name__ == '__main__':
    unittest.main()
