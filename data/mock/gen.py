import os
import random
from os.path import join

if __name__ == '__main__':
    base_dir = './user_appid_open_feature'
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
