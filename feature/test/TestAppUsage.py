import json
import unittest

from feature import AppUsage


class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.app_usage = AppUsage()
        with open('data.json', 'r') as f:
            conf = json.load(f)
            self.base_dir = conf['base_dir']
            self.app_usage.load_data_from_base_dir(self.base_dir)

    def test_get_topk_open_appid(self):
        result = self.app_usage.get_topk_open_appid(2)
        self.assertEqual(2, len(result))
        self.assertTrue('app3' in result.keys())
        self.assertTrue('app4' in result.keys())

    def process_record(self, user_id, app_id, count, duration, date):
        if user_id == 'user3' and app_id == 'app3':
            self.rec.append({
                'user_id': user_id,
                'app_id': app_id,
                'count': count,
                'duration': duration,
                'date': date
            })

    def on_end_of_one_file(self, origin_file_name):
        self.file_num += 1

    def test_extract_record(self):
        self.rec = []
        self.file_num = 0
        self.app_usage.scan_record(self.process_record, self.on_end_of_one_file)

        self.assertLess(0, self.file_num)
        self.assertEqual(1, len(self.rec))
        self.assertEqual('user3', self.rec[0]['user_id'])
        self.assertEqual('app3', self.rec[0]['app_id'])
        self.assertEqual(3, self.rec[0]['count'])
        self.assertEqual(3, self.rec[0]['duration'])
        self.assertEqual('2016-08-03', self.rec[0]['date'])

    def test_load_app_id(self):
        app_id_list = self.app_usage.load_app_id(self.base_dir)
        self.assertEqual(4, len(app_id_list))
        print app_id_list
        for i in xrange(1, 5):
            self.assertTrue('app' + str(i) in app_id_list)

if __name__ == '__main__':
    unittest.main()
