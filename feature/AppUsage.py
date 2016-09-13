import heapq
import json
from operator import itemgetter

from os import listdir

from os.path import isfile, join


class AppUsage:
    # app_id -> {user_id, count, duration, time}
    app_usage_data = {}

    def __init__(self):
        self.app_usage_data = {}

    def addAppIdInfo(self, user_id, app_id, count, duration, date):
        if app_id not in self.app_usage_data:
            self.app_usage_data[app_id] = {'open_sum': 0L,
                                           'duration_sum': 0L}
        self.app_usage_data[app_id]['open_sum'] += long(count)
        self.app_usage_data[app_id]['duration_sum'] += long(duration)

    def getTopKOpenAppid(self, k):
        return self.getTopKByKey(k, 'open_sum')

    def getTopKByKey(self, k, key_name):
        return heapq.nlargest(k, self.app_usage_data.items(), key=lambda x:x[1][key_name])


if __name__ == '__main__':
    app_usage = AppUsage()
    with open('AppUsage.json', 'r') as f:
        conf = json.load(f)
        data_path = conf['data_dir']
        files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
        for f in files:
            with open(join(data_path, f)) as data_file:
                for line in data_file.readlines():
                    user_id, app_id, count, duration, time = line.split()
                    app_usage.addAppIdInfo(user_id=user_id, app_id=app_id, count=count, duration=duration, date=time)

        k = conf['topK']
        topK = app_usage.getTopKOpenAppid(k)
        ii = 0
        for i in topK:
            print ii
            ii = 1 + ii
            print '\n'
            print i
            print '\n'

