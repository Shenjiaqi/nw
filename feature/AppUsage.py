import heapq
import json
import os

from os import listdir

from os.path import isfile, join


class AppUsage:
    # {app_id: {count, duration, time}}
    app_usage_data = {}

    def __init__(self):
        self.app_usage_data = {}
        self.app_usage_data_files = []

    def add_app_id_info(self, user_id, app_id, count, duration, date):
        if app_id not in self.app_usage_data:
            self.app_usage_data[app_id] = {'open_sum': 0L,
                                           'duration_sum': 0L,
                                           'day_sum': 0L}
        self.app_usage_data[app_id]['open_sum'] += long(count)
        self.app_usage_data[app_id]['duration_sum'] += long(duration)
        self.app_usage_data[app_id]['day_sum'] += 1

    def get_topk_open_appid(self, k):
        # {app_id: {open_sum, duration_sum, day_sum}}
        return self.get_topk_by_key(k, 'open_sum')

    def get_topk_by_key(self, k, key_name):
        r = heapq.nlargest(k, self.app_usage_data.items(), key=lambda x:x[1][key_name])
        # convert to dict
        ret = {}
        for i in r:
            ret[i[0]] = i[1]
        return ret

    def load_file(self, data_path):
        for f in [f for f in listdir(data_path) if isfile(join(data_path, f))]:
            file_path = join(data_path, f);
            self.app_usage_data_files.append(file_path)
            with open(file_path) as data_file:
                for line in data_file.readlines():
                    user_id, app_id, count, duration, time = line.split()
                    self.add_app_id_info(user_id=user_id,
                                         app_id=app_id,
                                         count=count,
                                         duration=duration,
                                         date=time)

    def load_data_from_base_dir(self, base_dir):
        self.load_file(join(base_dir, "contest_dataset_app_usage"))

    def scan_record(self, process_record, on_end_of_one_file):
        for file in self.app_usage_data_files:
            with open(file, 'r') as f:
                for line in f.readlines():
                    user_id, app_id, count, duration, time = line.split()
                    process_record(user_id=user_id,
                                   app_id=app_id,
                                   count=long(count),
                                   duration=long(duration),
                                   date=time)
                on_end_of_one_file(os.path.basename(file))


if __name__ == '__main__':
    app_usage = AppUsage()
    with open("data.json", 'r') as f:
        conf = json.load(f)
        base_dir = conf['data_base']
        k = conf['app_usage_topK']
        app_usage.load_data_from_base_dir(base_dir=base_dir)
        topK = app_usage.get_topk_open_appid(k)
        ii = 0
        for i in topK:
            print ii
            ii = 1 + ii
            print i
            print '\n'

