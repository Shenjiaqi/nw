import json
from os import listdir
from os.path import join

from os.path import isfile


class UserLabel:
    def __init__(self):
        self.user_info = {}

    def add_user(self, user_id, gender, age_group):
        if user_id in self.user_info:
            print "Error duplicated user_id [", user_id, "]"
        else:
            self.user_info[user_id] = {"gender": int(gender),
                                       "age_group": int(age_group)}

    def get_user_list(self):
        return self.user_info.keys()

    def load_data(self, data_path):
        files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
        for f in files:
            with open(join(data_path, f)) as data_file:
                for line in data_file:
                    user_id, gender, age_group = line.split()
                    self.add_user(user_id=user_id, gender=gender, age_group=age_group)

    def get_user(self, user_id):
        if user_id in self.user_info:
            return self.user_info[user_id]
        return None

    def load_data_from_base_dir(self, base_dir):
        self.load_data(join(base_dir, 'contest_dataset_label'))

if __name__ == '__main__':
    user_label = UserLabel()
    with open("data.json", "r") as f:
        conf = json.load(f)
        user_label.load_data_from_base_dir(conf['base_dir'])


