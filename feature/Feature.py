import json

from feature import AppUsage
from feature.UserLabel import UserLabel


class Feature:
    def __init__(self):
        self.app_usage = AppUsage()
        self.user_label = UserLabel()

    def load_data(self, conf):
        base_dir = conf['base_dir']
        self.app_usage.load_data_from_base_dir(base_dir=base_dir)
        self.user_label.load_data_from_base_dir(base_dir=base_dir)

    def generate_user_feature_by_topk_open_appid(self, k):
        topk_appid_dict = self.app_usage.get_topk_open_appid(k)
        user_id_list = self.user_label.get_user_list()
        user_id_dict = {}
        for u in user_id_list:
            user_id_dict[u] = None
        rec = self.app_usage.extract_record(lambda user_id,
                                                   app_id,
                                                   count,
                                                   duration,
                                                   date: user_id in user_id_dict and
                                                         app_id in topk_appid_dict)
        # {user_id: {app_id: {duration, day_sum, open_sum}}}
        user_rec = {}
        for k in rec:
            user_id = k['user_id']
            app_id = k['app_id']
            duration = long(k['duration'])
            date = k['date']
            count = long(k['count'])
            if user_id not in user_rec:
                user_rec[user_id] = {}
            if app_id not in user_rec[user_id]:
                user_rec[user_id][app_id] = {'duration': 0L,
                                             'day_sum': 0L,
                                             'open_sum': 0L}
            user_rec[user_id][app_id]['duration'] += duration
            user_rec[user_id][app_id]['day_sum'] += 1
            user_rec[user_id][app_id]['open_sum'] += count

        for u in user_rec.keys():
            for a in user_rec[u]:
                user_rec[u][a]['avg_open_cnt'] = float(user_rec[u][a]['open_sum']) / \
                                                 float(user_rec[u][a]['day_sum'])
                user_rec[u][a]['avg_duration_time'] = float(user_rec[u][a]['duration']) / \
                                                      float(user_rec[u][a]['day_sum'])

        # normalization
        for app_id in topk_appid_dict.keys():
            app_id_avg_open_sum = 0.0
            app_id_avg_duration_sum = 0.0
            for user in user_rec.keys():
                if app_id in user_rec[user]:
                    app_id_avg_duration_sum += user_rec[user][app_id]['avg_duration_time']
                    app_id_avg_open_sum += user_rec[user][app_id]['avg_open_cnt']
            for u in user_rec.keys():
                if app_id in user_rec[u]:
                    user_rec[u][app_id]['norm_duration'] = 0 if app_id_avg_duration_sum < 1e-7 else \
                        user_rec[u][app_id]['avg_duration_time'] / app_id_avg_duration_sum
                    user_rec[u][app_id]['norm_open_cnt'] = 0 if app_id_avg_open_sum < 1e-7 else \
                        user_rec[u][app_id]['avg_open_cnt'] / app_id_avg_open_sum

        # print result
        for u in user_rec.keys():
            d = self.user_label.get_user(u)
            print d['gender'], ' ', d['age_group'], ' ',
            for a in topk_appid_dict.keys():
                if a in user_rec[u]:
                    print user_rec[u][a]['norm_duration'], ' ', user_rec[u][a]['norm_open_cnt'], ' ',
                else:
                    print 0.0, ' ', 0.0, ' ',
            print ''

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        feature = Feature()
        feature.load_data(conf)
        feature.generate_user_feature_by_topk_open_appid(conf['app_usage_topK'])






