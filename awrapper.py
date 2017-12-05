# -*- coding: utf-8 -*-

import argparse
import subprocess
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--version', help='display version number and exit', action='version', version='%(prog)s 0.3.1')
parser.add_argument('-c', '--config', help='specify configuration file for script', type=argparse.FileType('r'), required=True)

args = parser.parse_args()

def dirman(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

content = []
cdict = {}
dl = []
ql = []

with args.config as f:
    content = f.readlines()
    
for r in content:
    if r[0] != '#':
        cdict[r.split(':')[0].strip()] = r.split(':')[1].strip()

print cdict
    

dirman(cdict['odir'])
dirman(cdict['temp_dir'])

dirs = [d for d in os.listdir(cdict['basedir']) if os.path.isdir(os.path.join(cdict['basedir'], d))]


for d in dirs:

    for r in os.walk(cdict['basedir']+'/'+d).next()[2]:
        dl.append(d+'/'+r)
    dirman(cdict['odir']+'/'+d)
    dirman(cdict['temp_dir']+'/'+d)

cdict['fl'] = dl


subprocess.call(['luigi', '--module', 'aflow', 'map_damage', '--config', json.dumps(cdict)])
