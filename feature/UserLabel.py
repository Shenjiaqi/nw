import json
import random
from os import listdir
from os.path import join

from os.path import isfile


class UserLabel:
    def __init__(self):
        self.user_info = {}
        self.user_counter = 0

    def add_user(self, user_id, gender, age_group):
        if user_id in self.user_info:
            print "Error duplicated user_id [", user_id, "]"
        else:
            self.user_info[user_id] = (gender, age_group)
            self.user_counter += 1

    def get_user_list(self):
        return self.user_info.keys()

    def load_data(self, data_path):
        files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
        for f in files:
            with open(join(data_path, f)) as data_file:
                for line in data_file:
                    user_id, gender, age_group = line.strip().split()
                    self.add_user(user_id=user_id, gender=gender, age_group=age_group)

    def get_user(self, user_id):
        if user_id in self.user_info:
            return self.user_info[user_id]
        return None

    def get_user_age(self, user_id):
        return int(self.user_info[user_id][1])

    def get_user_gender(self, user_id):
        return int(self.user_info[user_id][0])

    def load_data_from_base_dir(self, base_dir):
        self.load_data(base_dir)

    def load_gender_data(self, data_dir):
        file_path = [join(data_dir, 'contest_dataset_label', f)
                     for f in listdir(join(data_dir, 'contest_dataset_label'))]
        print file_path
        user_dict = {}
        for file in file_path:
            with open(file, 'r') as f:
                for line in f:
                    user_id, gender, age_group = line.strip().split()
                    gender = int(gender)
                    age_group = int(age_group)
                    if gender not in user_dict:
                        user_dict[gender] = []
                    user_dict[gender].append(user_id)
        return user_dict

    def filter_user_dict(self, user_dict, n):
        for i in user_dict.keys():
            l = len(user_dict[i])
            for j in xrange(0, l):
                o = random.randint(j, l - 1)
                if o != j:
                    tmp = user_dict[i][o]
                    user_dict[i][o] = user_dict[i][j]
                    user_dict[i][j] = tmp
                if j >= n:
                    user_dict[i] = user_dict[i][0:n]
                    break
    def load_gender_data_less_than(self, data_dir, n):
        user_dict = self.load_gender_data(data_dir=data_dir)
        self.filter_user_dict(user_dict, n)
        return user_dict

    def load_age_data(self, data_dir):
        file_path = [join(data_dir, 'contest_dataset_label', f)
                     for f in listdir(join(data_dir, 'contest_dataset_label'))]
        user_dict = {}
        for file in file_path:
            with open(file, 'r') as f:
                for line in f:
                    user_id, gender, age_group = line.strip().split()
                    gender = int(gender)
                    age_group = int(age_group)
                    if age_group not in user_dict:
                        user_dict[age_group] = []
                    user_dict[age_group].append(user_id)
        return user_dict

    def load_age_data_less_than(self, data_dir, n):
        user_dict = self.load_age_data(data_dir=data_dir)
        self.filter_user_dict(user_dict, n)
        return user_dict

if __name__ == '__main__':
    user_label = UserLabel()
    with open("data.json", "r") as f:
        conf = json.load(f)
        user_label.load_data_from_base_dir(conf['base_dir'])


