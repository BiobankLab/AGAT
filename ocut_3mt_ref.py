# -*- coding: utf-8 -*-

import luigi
import subprocess
import json
import gzip
#from multiprocessing.pool import Pool
from pathos.multiprocessing import ProcessingPool as Pool


class utils():
    @staticmethod
    def fname(fn):
        tf = fn.split('/')
        if len(tf) > 1:
            fname = tf[-1]
        else:
            fname = r
        return fname
    
    @staticmethod
    def rem_suffix(fname):
        return ('.').join(fname.split('.')[:-1])
    
    @staticmethod
    def executor(cmd):
        subprocess.call(cmd)
        #print cmd
    
    @staticmethod
    def executor_pipe(cmd):
        sout = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        cnt = sout.stdout.read()
        print 'saving to: '+cmd[-2]+'-stat.txt'
        with open(cmd[-2]+'-stat.txt', 'wb') as w:
            w.write(cnt)

                    

class bwa_index(luigi.Task):
    # -a algorithm [is|bwtsw]
    # -p file prefix?
    # input file db.fq
    
    #bwa index
    
    config = luigi.DictParameter()
    
    def requires(self):
        return []
    
    def output(self):
        return [luigi.LocalTarget(utils.rem_suffix(self.config['clear-ref'])+'.amb'),
            luigi.LocalTarget(utils.rem_suffix(self.config['clear-ref'])+'.ann'),
            luigi.LocalTarget(utils.rem_suffix(self.config['clear-ref'])+'.bwt'),
            luigi.LocalTarget(utils.rem_suffix(self.config['clear-ref'])+'.pac'),
            luigi.LocalTarget(utils.rem_suffix(self.config['clear-ref'])+'.sa')]
        
    def run(self):
        cmd = ['bwa', 'index']
        if self.config['algorithm']:
            cmd = cmd+['-a', self.config['algorithm']]
        cmd = cmd + ['-p', utils.rem_suffix(self.config['clear-ref']), self.config['clear-ref']]
        subprocess.call(cmd)
        
    
class bwa_aln(luigi.Task):
    # -l seed len 
    # threads
    # ref
    # infile - result of merge_file
    #bwa aln 
    
    config = luigi.DictParameter()
    
    def requires(self):
        return [bwa_index(self.config)]
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sai'))
        return lst
    
    def run(self):
        cmd_base = ['bwa', 'aln']
        if self.config['seedlen'] > 0:
            cmd_base = cmd_base + ['-l', self.config['seedlen'] ]
        if self.config['threads'] > 0:
            cmd_base = cmd_base + ['-t', self.config['threads']]
        for s in self.config['samples']:
            cmd = cmd_base + [utils.rem_suffix(self.config['clear-ref']), s['files'][0]] #suffix dependant from imput


            with open(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sai', 'w') as f:
                subprocess.call(cmd, stdout=f)
            cmd = []
        
class bwa_samse(luigi.Task):
    # db.fasta
    # sai from baw_aln task
    # output file
    #bwa samse 
    
    config = luigi.DictParameter()
    
    @staticmethod
    def exec_samse(files):
        cmd = ['bwa', 'samse', files['ref'], files['sai'], files['fq']]
        with open(files['sam'], 'w') as f:
            subprocess.call(cmd, stdout = f)
    
    def requires(self):

        return [bwa_aln(self.config)]
        
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sam'))
        return lst
        
    def run(self):
        fl = []
        
        cmd_base = ['bwa', 'samse', utils.rem_suffix(self.config['clear-ref'])]
        for s in self.config['samples']:
            l ={}
            l['ref'] = utils.rem_suffix(self.config['clear-ref'])
            l['sai'] = self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sai'
            l['fq'] = s['files'][0]#self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.fastq'
            l['sam'] = self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sam'
            fl.append(l)

            
        with Pool(int(self.config['threads'])) as p:
            p.map(self.exec_samse, fl)        

########################

class unmaped(luigi.Task):
    config = luigi.DictParameter()
    
    @staticmethod
    def exec_unmaped(files):
        cmd = ['samtools', 'view', '-f', '4', '-Sb', files['sam']]
        with open(files['bam'], 'w') as f:
            subprocess.call(cmd, stdout = f)
    
    def requires(self):
        return [bwa_samse(self.config)]
        
    def run(self):

        fl = []
        for s in self.config['samples']:
            fl.append({'sam':self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sam', 'bam':self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.bam'})
        with Pool(int(self.config['threads'])) as p:
            p.map(self.exec_unmaped, fl)  
    
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.bam'))
        return lst

class sam2fq(luigi.Task):

    config = luigi.DictParameter()
    
    @staticmethod
    def sam2fq_exec(files):
        cmd = ['samtools', 'bam2fq', files['in']]
        with open(files['out'], 'w') as f:
            subprocess.call(cmd, stdout = f)
    
    def requires(self):
        return [unmaped(self.config)]
        
    def run(self):
        fl = []
        for s in self.config['samples']:
           fl.append({'out':self.config['odir'].rstrip('/')+'/'+s['name']+'.fastq', 'in':self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.bam'})
        with Pool(int(self.config['threads'])) as p:
            p.map(self.sam2fq_exec, fl)
        
    def output(self):
        lst = []
        clist = []
        for s in self.config['samples']:
            clist.append(self.config['odir'].rstrip('/')+'/'+s['name']+'.fastq')
            lst.append(luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+s['name']+'.fastq'))
        print clist
        return lst
