# -*- coding: utf-8 -*-

import luigi
import subprocess
import json
import gzip
import os
from pathos.multiprocessing import ProcessingPool as Pool

'''
    adapter:type of adapter
    quality:quality
    qb:quality base (phred)
    minlen:min len of contig to be valid
    flist:file list to be proceed
    odir:output dir
    temp_dir:dir for temp files schould be removed after all.
    target_file_name:name of target file
    algorithm: algorithm for indexing
    fprefix: file prefix for indexed
    ref_file: reference file
    sedlen
    cores
'''

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
    def exec_stats(fl):
        with open(fl['output'],'w') as f:
            subprocess.call(['samtools', 'flagstat', fl['bam']], stdout=f)
            
            
class bwa_index(luigi.Task):
    # -a algorithm [is|bwtsw]
    # -p file prefix?
    # input file db.fq
    
    #bwa index 
    
    config = luigi.DictParameter()
    
    def requires(self):
        return []
    
    def output(self):
        return [luigi.LocalTarget(self.config['fprefix']+'.amb'),
            luigi.LocalTarget(self.config['fprefix']+'.ann'),
            luigi.LocalTarget(self.config['fprefix']+'.bwt'),
            luigi.LocalTarget(self.config['fprefix']+'.pac'),
            luigi.LocalTarget(self.config['fprefix']+'.sa')]
        
    def run(self):
        cmd = ['bwa', 'index']
        if self.config['algorithm']:
            cmd = cmd+['-a', self.config['algorithm']]
        if self.config['fprefix']:
            cmd = cmd + ['-p', self.config['fprefix']]
        cmd = cmd + [self.config['ref_file']]
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
        for r in self.config['fl']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.sai'))
        return lst 
    
    def run(self):
        cmd_base = ['bwa', 'aln']
        if self.config['seedlen'] > 0:
            cmd_base = cmd_base + ['-l', self.config['seedlen'] ]
        if self.config['cores'] > 0:
            cmd_base = cmd_base + ['-t', self.config['cores']]
        cmd_base = cmd_base + [self.config['fprefix']]
        for r in self.config['fl']:
            cmd = cmd_base + [self.config['basedir'].rstrip('/')+'/'+r]
                     


            with open(self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.sai', 'w') as f:
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
        for r in self.config['fl']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.sam'))
        return lst
        
        
        
    def run(self):
        fl = []
        for r in self.config['fl']:
            l = {}
            l['ref'] = utils.rem_suffix(self.config['ref_file'])
            l['sai'] = self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.sai'
            l['fq'] = self.config['basedir'].rstrip('/')+'/'+r
            l['sam'] = self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.sam'
            fl.append(l)
        with Pool(int(self.config['cores'])) as p:
            p.map(self.exec_samse, fl)
            
            
class sam_2_bam(luigi.Task):
    
    #samtools view
    
    config = luigi.DictParameter()
    
    
    def requires(self):
        return bwa_samse(self.config)
    def output(self):
        lst = []
        for r in self.config['fl']:
            lst.append(luigi.LocalTarget(self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'_with_dups.bam'))
        return lst
    
    def run(self):
        cmd_base = ['samtools', 'view', '-Sb', '-t', self.config['cores']]
            
        for r in self.config['fl']:
            cmd = cmd_base + [self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.sam']
            with open(self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'_with_dups.bam', 'w') as f:
                subprocess.call(cmd, stdout=f)
            cmd = []

class stats_wd(luigi.Task):  
    
    config = luigi.DictParameter()
    
    
    def requires(self):
        return sam_2_bam(self.config)
        
    def output(self):
        print 'wd otp'
        lst = []
        for r in self.config['fl']:
            lst.append(luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.stats_with_dups.txt'))
        return lst
        
        
    def run(self):
        fl = []
        for r in self.config['fl']:
            l = {}
            l['output'] = self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.stats_with_dups.txt'
            l['bam'] = self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'_with_dups.bam'
            fl.append(l)
        
        with Pool(int(self.config['cores'])) as p:
            p.map(utils.exec_stats, fl)        
        
class remove_dups(luigi.Task):
    
    config = luigi.DictParameter()
    
    @staticmethod
    def rmdup_exec(f):
        cmd = ['samtools', 'rmdup', f['in'], f['out']]
        subprocess.call(cmd)
    
    def requires(self):
        return stats_wd(self.config)
    
    def output(self):
        lst = []
        for r in self.config['fl']:
            lst.append(luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.bam'))
        return lst
    
    def run(self):
        fl = []
        for r in self.config['fl']:
            fl.append({'in':self.config['temp_dir'].rstrip('/')+'/'+utils.rem_suffix(r)+'_with_dups.bam', 'out':self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.bam'})
            
        with Pool(int(self.config['cores'])) as p:
            p.map(remove_dups.rmdup_exec, fl)     

class statistics(luigi.Task):   
    config = luigi.DictParameter()
    def requires(self):
        return remove_dups(self.config)
    
    def output(self):
        lst = []
        for r in self.config['fl']:
            lst.append(luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.stats.txt'))
        return lst
        
    def run(self):

        fl = []
        for r in self.config['fl']:
            l = {}
            l['output'] = self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.stats.txt'
            l['bam'] = self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.bam'
            fl.append(l)
        
        with Pool(int(self.config['cores'])) as p:
            p.map(utils.exec_stats, fl)
        
class map_damage(luigi.Task):
   
    config = luigi.DictParameter()
    
    @staticmethod
    def exec_md(f):
        os.chdir(f['dir'])
        cmd = ['mapDamage', '-i', f['in'], '-r', f['ref']]  
        subprocess.call(cmd)
      
    def requires(self):
        return statistics(self.config)
    
    def output(self):
        lst = []
        for r in self.config['fl']:
            tname = utils.rem_suffix(r).split('/')
            
            lst += [
                luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+tname[0]+'/results_'+tname[-1]+'/Fragmisincorporation_plot.pdf'), 
                luigi.LocalTarget(self.config['odir'].rstrip('/')+'/'+tname[0]+'/results_'+tname[-1]+'/Length_plot.pdf')
            ]
            
        return lst
    
    def run(self):
        fl = []
        for r in self.config['fl']:
            fl.append({'dir':self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r).split('/')[0],
                'in':self.config['odir'].rstrip('/')+'/'+utils.rem_suffix(r)+'.bam',
                'ref':self.config['ref_file']
            })
        with Pool(int(self.config['cores'])) as p:
            p.map(map_damage.exec_md, fl)    
