import json
import os
from os.path import join

if __name__ == '__main__':

    with open("data.json", "r") as f:
        conf = json.load(f)
        device_dir = join(conf['base_dir'], conf['reduced_app_usage'])
        device_dir_out = join(conf['uv_dir'], 'app_usage')
        app_id_dict = {}
        app_id_cnt = 0
        with open(device_dir_out, 'w') as out_f:
            for i in os.listdir(device_dir):
                with open(join(device_dir, i), 'r') as f:
                    for line in f:
                        user_id, app_id, count, duration = line.strip().split()
                        if app_id not in app_id_dict:
                            app_id_dict[app_id] = app_id_cnt
                            app_id_cnt += 1
                        app_id_idx = app_id_dict[app_id]
                        out_f.write('\t'.join([user_id, 'ac' + app_id_idx, count]) + '\n')
                        out_f.write('\t'.join([user_id, 'ad' + app_id_idx, duration]) + '\n')
