import json
import os
from os.path import join

if __name__ == '__main__':

    with open("data.json", "r") as f:
        conf = json.load(f)
        device_dir = join(conf['base_dir'], conf['contest_dataset_device_info'])
        device_dir_out = join(conf['uv_dir'], 'device_info')
        brand_dict = {}
        brand_cnt = 0
        model_dict = {}
        model_cnt = 0
        with open(device_dir_out, 'w') as out_f:
            for i in os.listdir(device_dir):
                with open(join(device_dir, i), 'r') as f:
                    for line in f:
                        user_id, brand, model = line.strip().split()
                        if brand not in brand_dict:
                            brand_dict[brand] = brand_cnt
                            brand_cnt += 1
                        if model not in model_dict:
                            model_dict[model] = model_cnt
                            model_cnt += 1
                        out_f.write('\t'.join([user_id, 'brand', str(brand_dict[brand])]) + '\n')
                        out_f.write('\t'.join([user_id, 'model', str(model_dict[model])]) + '\n')
