# -*- coding: utf-8 -*-

import argparse
import subprocess
import json
import os
import pprint

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--version', help='display version number and exit', action='version', version='%(prog)s 0.3.1')
parser.add_argument('-c', '--config', help='specify configuration file for script', type=argparse.FileType('r'), required=True)

args = parser.parse_args()

def dirman(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

content = []
cdict = {'samples':[]}
fl = []
ql = []

with args.config as f:
    content = f.readlines()
    
for r in content:
    if r[0] != '#':
        if r.split(':')[0].strip() == 'sample':
            print '\nadding sample'
            temp = r.split(':')[1].split(',')
            j = {'name':temp[0].strip(), 'files':[]}
            for k in temp[1:]:
                j['files'].append(k.strip())
            cdict['samples'].append(j)
            j = {}
        else:
            #cdict['samples'].append({'name':r.split(',')[0],'files':r.split}
            #print r
            cdict[r.split(':')[0].strip()] = r.split(':')[1].strip()

print cdict
    
#for r in cdict['flist'].split(','):
#    fl.append(r.strip())
    
cdict['flist'] = fl

for r in cdict['minlen'].split(','):
    ql.append(r.strip())

cdict['minlen'] = ql
#how to determine if luigid is running?
#subprocess.Popen(['luigid'])

dirman(cdict['odir'])
dirman(cdict['temp_dir'])
dirman(cdict['temp_dir']+'/clearing')

for q in cdict['minlen']:
    dirman(cdict['odir']+'/'+str(q))
    dirman(cdict['temp_dir']+'/'+str(q))
    
for s in cdict['samples']:
    dirman(cdict['temp_dir']+'/clearing/'+s['name'])
    

print '\n\n\n'
pp = pprint.PrettyPrinter(indent=4)            
pp.pprint(cdict)
print ["PYTHONPATH='/home/blul/BIOIT/dwf'", 'luigi', '--module', 'dwf', 'map_damage', json.dumps(cdict)]
subprocess.call(['luigi', '--module', 'ocut_3mt', 'cutadapt', '--config', json.dumps(cdict)])#cutadapt
