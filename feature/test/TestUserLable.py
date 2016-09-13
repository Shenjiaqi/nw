import json
import unittest

from feature.UserLabel import UserLabel


class TestUserLabel(unittest.TestCase):
    def setUp(self):
        self.user_label = UserLabel()
        with open('data.json', 'r') as f:
            conf = json.load(f)
            self.user_label.load_data_from_base_dir(conf['base_dir'])

    def test_get_user_list(self):
        user_list = self.user_label.get_user_list()
        self.assertEqual(4, len(user_list))

    def test_get_user(self):
        self.assertEqual(None, self.user_label.get_user("not_exist"))
        self.assertEqual({'gender': 1, 'age_group': 1}, self.user_label.get_user('user1'))

    def test_get_user_list(self):
        self.assertEqual(4, len(self.user_label.get_user_list()))
        self.assertTrue('user1' in self.user_label.get_user_list())
        self.assertTrue('user2' in self.user_label.get_user_list())
        self.assertTrue('user3' in self.user_label.get_user_list())
        self.assertTrue('user4' in self.user_label.get_user_list())

if __name__ == '__main__':
    unittest.main()