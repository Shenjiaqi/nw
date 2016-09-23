
import json
import os.path

def get_appid_feature_idx(counter, appid_idx_dict, lines):
    for l in lines:
        app_id = l.strip()
        if app_id not in appid_idx_dict:
            counter += 1
            appid_idx_dict[app_id] = counter

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        uv_dir = join(base_dir, conf['uv_dir'])
        uv_top100_appid_dir = join(uv_dir, conf['usage_top100_appid'])
        uv_install_top100_appid_dir = join(uv_dir, conf['install_top100_appid'])
        uv_query_top100_dir = join(uv_dir, conf['query_top100'])
        uv_url_top100_dir = join(uv_dir, conf['url_top100'])
        

