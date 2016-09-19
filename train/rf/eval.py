import json
import os
import random
from os.path import join

import pickle

from RFTrainer import RFTrainer
from train import FeatureLoader

if __name__ == '__main__':
    with open('data.json', 'r') as f:
        conf = json.load(f)
        base_dir = conf['base_dir']
        model_dir = join(base_dir, conf['model_dir'])
        feature_loader = FeatureLoader()
        feature_list = feature_loader.load_age_data_less_than_n(base_dir=base_dir, n=10000)
        rf_trainer = RFTrainer()
        rf_trainer.load_model(model_dir=model_dir)
        for i in feature_list:
            cnt = [0, 0]
            for j in feature_list[i]:
                pred = rf_trainer.predict_age(j)
                mk = 0
                for k in len(pred):
                    if pred[mk] < pred[k]:
                        mk = k
                cnt[mk] += 1
                if (cnt[0] + cnt[1] % 100) == 0:
                    print i, cnt
            print "#####", i, cnt
        '''
        feature_dir = join(base_dir, conf['norm_feature_dir'])
        eval_dir = join(base_dir, conf['eval_dir'])
        rf_trainer = RFTrainer()
        rf_trainer.load_model(model_dir)
        # age, gender
        for t in os.listdir(feature_dir):
            # age/1, age/2, gender/1, ...
            for c in os.listdir(join(feature_dir, t)):
                # age/1/feature
                for file in os.listdir(join(feature_dir, t, c)):
                    print join(feature_dir, t, c, file)
                    with open(join(feature_dir, t, c, file), 'r') as f:
                        if not os.path.exists(join(eval_dir, t, c)):
                            os.makedirs(join(eval_dir, t, c))
                        with open(join(eval_dir, t, c, file), 'w') as out_f:
                            pred_cnt = [0 for i in range(0, 8)]
                            pred_sum = 0
                            for l in f.readlines():
                                if random.randint(0, 10) == 0:
                                    feature_line = [float(i) * 1e7 for i in l.split(',')]
                                    if t == 'age':
                                        pred_result = rf_trainer.predict_age([feature_line])[0]
                                    else:
                                        pred_result = rf_trainer.predict_gender([feature_line])[0]
                                    out_f.write(','.join([str(x) for x in pred_result]) + '\n')
                                    jdx = 0
                                    for j in xrange(0, len(pred_result)):
                                        if pred_result[jdx] < pred_result[j]:
                                            jdx = j
                                    pred_sum += 1
                                    pred_cnt[jdx] += 1
                                    if pred_sum % 100 == 0:
                                        print ','.join([str(x / float(pred_sum + 0.001)) for x in pred_cnt])
                            print t, c, file
                            print ','.join([str(x / float(pred_sum + 0.001)) for x in pred_cnt])
        '''
