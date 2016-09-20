import json
import unittest
from os.path import join

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

    def nxt(self, a):
        for i in xrange(len(a) - 1, 0, -1):
            if a[i - 1] < a[i]:
                mk = i
                for k in xrange(i, len(a)):
                    if a[i - 1] < a[k] < a[mk]:
                        mk = k
                tmp = a[i - 1]
                a[i - 1] = a[mk]
                a[mk] = tmp
                a[i:] = sorted(a[i:])
                return a
        return None

    def tst(self, ia, ib, arr0, m2, sum_open, sum_time):
        for i in xrange(0, 4):
            for j in xrange(0, 4):
                aa = arr0[i][j * 2] / sum_open[j]
                bb = arr0[i][j * 2 + 1] / sum_time[j]
                cc = m2[ia[i], ib[j] * 2]
                dd = m2[ia[i], ib[j] * 2 + 1]
                if abs(aa - cc) > 1e-7 or abs(bb - dd) > 1e-7:
                    return False
        return True

    def test_load_gender_data(self):
        feature_dir = self.conf['feature_dir']
        m2, t2 = self.feature_loader.load_data_less_than_n(self.base_dir, feature_dir, 2, 'age')
        print m2, t2

        strs = 'user1 app1 1.1 1.1\nuser1 app2 1.2 1.2\n' \
               'user1 app3 1.3 1.3\nuser1 app4 1.4 1.4\n' \
               'user2 app1 2.1 2.1\nuser2 app2 2.2 2.2\n' \
               'user2 app3 2.3 2.3\nuser2 app4 2.4 2.4\n' \
               'user3 app3 3.1 3.1\nuser3 app4 3.2 3.2\n' \
               'user4 app3 4.3 4.3\nuser4 app4 4.4 4.4'

        sum_open = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        sum_time = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        arr0 = [[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]]
        for i in strs.split('\n'):
            user_id, app_id, open_avg, time_avg = i.strip().split()
            x = int(user_id.split('r')[1]) - 1
            y = int(app_id.split('p')[2]) - 1
            arr0[x][y * 2] += float(open_avg)
            arr0[x][y * 2 + 1] += float(time_avg)
            open_avg = float(open_avg)
            time_avg = float(time_avg)
            sum_open[y] += open_avg
            sum_time[y] += time_avg

        self.assertEqual(4, m2.shape[0])
        self.assertEqual(8, m2.shape[1])

        ia = [0, 1, 2, 3]
        ok = False
        while ia:
            ib = [0, 1, 2, 3]
            while ib:
                if self.tst(ia, ib, arr0, m2, sum_open, sum_time):
                    ok = True
                ib = self.nxt(ib)
            ia = self.nxt(ia)

        for i in arr0:
            j = 0
            while j < len(i):
                print i[j] / sum_open[j / 2], i[j + 1] / sum_time[j / 2],
                j += 2
            print ''
        for i in m2.toarray():
            for j in i:
                print j,
            print ''
        self.assertTrue(ok)



if __name__ == '__main__':
    unittest.main()