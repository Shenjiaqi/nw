import json
import unittest

from feature.UserLabel import UserLabel


class TestUserLabel(unittest.TestCase):
    def setUp(self):
        self.user_label = UserLabel()
        with open('data.json', 'r') as f:
            conf = json.load(f)
            self.base_dir = conf['base_dir']
            self.user_label.load_data_from_base_dir(self.base_dir)

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

    def test_load_gender_data(self):
        user_dict = self.user_label.load_gender_data(self.base_dir)
        self.assertTrue(1 in user_dict)
        self.assertTrue(2 in user_dict)
        self.assertTrue('user1' in user_dict[1])
        self.assertTrue('user3' in user_dict[1])
        self.assertTrue('user2' in user_dict[2])
        self.assertTrue('user4' in user_dict[2])

    def test_load_gender_data_less_than(self):
        user_dict = self.user_label.load_gender_data_less_than(self.base_dir, 1)
        self.assertEqual(2, len(user_dict))
        self.assertEqual(1, len(user_dict[1]))
        self.assertEqual(1, len(user_dict[2]))

        user_dict = self.user_label.load_gender_data_less_than(self.base_dir, 2)
        self.assertNotEqual(user_dict[1][0], user_dict[1][1])
        self.assertNotEqual(user_dict[2][0], user_dict[2][1])
        for i in xrange(1, 5):
            self.assertTrue('user' + str(i) in user_dict[1] or
                            'user' + str(i) in user_dict[2])

    def test_load_age_data_less_than(self):
        user_dict = self.user_label.load_age_data_less_than(self.base_dir, 2)
        self.assertEqual(3, len(user_dict))
        self.assertTrue('user1' in user_dict[1])
        self.assertTrue('user2' in user_dict[2] and
                        'user3' in user_dict[2])
        self.assertTrue('user4' in user_dict[4])

        user_dict = self.user_label.load_age_data_less_than(self.base_dir, 1)
        self.assertEqual(3, len(user_dict))
        self.assertTrue('user2' in user_dict[2])
        self.assertEqual(1, len(user_dict[2]))

if __name__ == '__main__':
    unittest.main()