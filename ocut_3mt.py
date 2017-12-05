# -*- coding: utf-8 -*-

import luigi
import subprocess
import json
import gzip

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

        
class merge_files(luigi.Task):
    
    config = luigi.DictParameter()
    
    @staticmethod
    def merge(s):
        fbody =''
        for r in s['in']:
                # if are zipped unzip them first
                print r
                if utils.fname(r).split('.')[-1] == 'gz':
                    with gzip.open(r, 'r') as f:
                        fbody += f.read()
                else:    
                    with open(r, 'r') as f:
                        fbody += f.read()
        with open(s['out'], 'w') as f:
            f.write(fbody)
    
    def requires(self):
        return []
        
    # merged file will be saved in output dir
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.fastq'))
        return lst

    def run(self):

        fl = []
        for s in self.config['samples']:

			fl.append({'out':self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.fastq', 'in':s['files']})
        with Pool(int(self.config['threads'])) as p:
            p.map(merge_files.merge, fl)
            

########################

        
class clear_cut(luigi.Task):

    config = luigi.DictParameter()
    
    def requires(self):
        return [merge_files(self.config)]
        
    def run(self):
        lst = []
        for s in self.config['samples']:
            #for r in s['files']:
                lst.append(["cutadapt", "-a", self.config['adapter'], '-q', self.config['clear-quality'], '--quality-base='+self.config['qb'], '--minimum-length', 
                    self.config['clear-len'], '-O', self.config['clear-overlap'], '-o', self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_clear.fastq', 
                    self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.fastq'
                ])
                
        p = Pool(int(self.config['threads']))
        p.map(utils.executor_pipe, lst)
            
                
        
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_clear.fastq'))
        return lst
            

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
    # sai output ? maybe not param?
    #bwa aln 
    
    config = luigi.DictParameter()
    
    def requires(self):
        return [bwa_index(self.config),clear_cut(self.config)]
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.sai'))
        return lst #  [luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+self.config['quality']+'/'+self.config['target_file_name']+'.sai')]
    
    def run(self):
        cmd_base = ['bwa', 'aln']
        if self.config['seedlen'] > 0:
            cmd_base = cmd_base + ['-l', self.config['seedlen'] ]
        if self.config['threads'] > 0:
            cmd_base = cmd_base + ['-t', self.config['threads']]
        for s in self.config['samples']:
            cmd = cmd_base + [utils.rem_suffix(self.config['clear-ref']), self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_clear.fastq'] #suffix dependant from imput


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
            l['fq'] = self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'.fastq'
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
           fl.append({'out':self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.fastq', 'in':self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.bam'})
        with Pool(int(self.config['threads'])) as p:
            p.map(self.sam2fq_exec, fl)
        
    def output(self):
        lst = []
        for s in self.config['samples']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.fastq'))
        return lst
        
class cutadapt(luigi.Task):
    # params:
    # Adapter (string)
    # qualit (int)
    # quality_base (int) (pherd)
    # len to skip (int)
    # file list (strings)
    # output dir name string - it can be temporarry? :)
    
    # fired for each file 
    
    config = luigi.DictParameter()
    
    def requires(self):
        return [sam2fq(self.config)]

    def output(self):
        lst = []
        for s in self.config['samples']:
            for q in self.config['minlen']:
                lst.append(luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+q+'/'+s['name']+'_cleared.fastq'))
        return lst

    def run(self):
        #preparing command to run, not in single line due to file list
        #if output dependant from quality and quality as list separate it in function!

        lst = []
        for s in self.config['samples']:
            for q in self.config['minlen']:
                    lst.append(["cutadapt", '--minimum-length', q, '-O', self.config['overlap'], "-a", self.config['adapter'],
                        '-o', self.config['odir'].rstrip('/')+'/'+q+'/'+s['name']+'.fastq', 
                        self.config['temp_dir'].rstrip('/')+'/clearing/'+s['name']+'/'+s['name']+'_cleared.fastq'
                    ])

                    #subprocess.call(cmd)
        with Pool(int(self.config['threads'])) as p:
            p.map(utils.executor_pipe, lst)
