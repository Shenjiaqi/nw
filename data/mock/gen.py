import os
import random
from os.path import join


def mk_dir(d):
    if not os.path.exists(d):
        os.makedirs(d)

if __name__ == '__main__':
    base_dir = './user_app_usage_feature_top100'
    user_dir = './contest_dataset_label'
    app_dir = './app_id'
    user_num = 1000
    app_num = 100
    user_gender = []
    user_age = []
    for i in xrange(0, user_num):
        user_gender.append(random.randint(1, 2))
        user_age.append(random.randint(1, 7))

    mk_dir(user_dir)
    with open(join(user_dir, 'user'), 'w') as f:
        for i in xrange(0, user_num):
            f.write('user' + str(i) + ' ' + str(user_gender[i]) + ' ' + str(user_age[i]) + '\n')

    mk_dir(app_dir)
    with open(join(app_dir, 'app'), 'w') as f:
        for i in xrange(0, app_num):
            # app open time and use time is not used
            f.write('app' + str(i) +
                    ' ' + str(random.randint(0, app_num)) +
                    ' ' + str(random.randint(0, user_num)) + '\n')

    mk_dir(base_dir)
    with open(join(base_dir, 'feature'), 'w') as f:
        for i in xrange(0, 1000):
            for j in xrange(0, 100):
                 f.write('user' + str(i) +
                         ' app' + str(j) +
                         ' ' + str(user_gender[i] * 100 + random.randint(0, 50)) +
                         ' ' + str(user_gender[i] * 10 + random.randint(0, 5)) + \
                         ' ' + str(user_gender[i]) + \
                         ' ' + str(user_age[i]) + \
                         ' ' + '123' + ' ' + '456' + '\n')

    '''
    # for age
    for i in xrange(1, 8):
        p = join(base_dir, 'age', str(i))
        if not os.path.exists(p):
            os.makedirs(p)
        with open(join(p, 'feature'), 'w') as f:
            for l in xrange(0, 500):
                v = []
                for j in xrange(0, 100):
                    v.append(i)
                f.write(','.join([str(x) for x in v]) + '\n')

    for i in xrange(1, 3):
        p = join(base_dir, 'gender', str(i))
        if not os.path.exists(p):
            os.makedirs(p)
        with open(join(p, 'feature'), 'w') as f:
            for l in xrange(0, 500):
                v = []
                for j in xrange(0, 100):
                    v.append(i)
                f.write(','.join([str(x) for x in v]) + '\n')
    '''
