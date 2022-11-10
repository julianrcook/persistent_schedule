# -*- coding: utf-8 -*-
""" run_safe_scheduler.py
Created on Wed Mar 24 20:05:16 2021
Server Wrapper for schedule, where jobs can be loaded from a config file
or spreadsheet or db table. XLSX only at the moment.
Thu Apr  1 modified to use SafeScheduler
@author: jucook
"""
# Schedule Library imported
import pandas as pd
import datetime
import time
import os
import subprocess
import logging
import csession
cluster_name = 'QR4Cluster'
database = 'synvol_data'
csession = csession.get_csession(cluster_name, database)
from cassandra.query import tuple_factory
csession.row_factory = tuple_factory
cas_fwrite = True
weekdays = (
                'monday',
                'tuesday',
                'wednesday',
                'thursday',
                'friday',
                'saturday',
                'sunday'
            )
pd.set_option('display.max_columns', 12) # Allows all columns to print on display
pd.set_option('display.width', 160) # as a service also requires mode con col=160
# This is  NT/Windows specific, os.getenv('HOSTNAME') is blank on Windows
# TODO multi os os.name == 'nt'
computer_name = os.getenv('COMPUTERNAME')
# =============================================================================
# Solution for unix is:
# import socket
# socket.gethostname()
# =============================================================================
def prepare_scheduledata():
                # returns prepared statement for repeated execution
    # csession is global
    insert_scheddata1 = 'insert into synvol_data.scheduler_config(scheduler_name,active,arg_list,at_time,depends_tag,do_job,every_freq,last_run,name_tag ,next_run,unit_freq) '
    # ('55-FEN-80272',1,'','11:45:00','','heston_data_to4',1,'2021-06-02 11:45:00','heston_tag','2021-06-03 11:45:00','days');
    insert_schedvalues = ' values(?,?,?,?,?,?,?,?,?,?,?)'
    insert_scheddata2 = insert_scheddata1 + insert_schedvalues
    return csession.prepare(insert_scheddata2)
 
#%% CallSubProcess for executing code in separate anaconda process
def CallSubProcess(pythonCmd):
    bat_cmd = 'py_1to4arg.bat '
    run_cmd = bat_cmd + pythonCmd.strip("'") # e.g. delay.py 10
    print('Trying ',run_cmd)
#==============================================================================
    p = subprocess.Popen(run_cmd,stdout = subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        stdout_result =  p.communicate()[0]
    except:
        stdout_result = [-1]
    return stdout_result
#==============================================================================
# TODO Because of the history index, each sub-process has to have a unique name (for now)
# At the moment 4 wrappers are created by default. If more than 4 subprocesses exist add more
def CallSubProcess01(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess02(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess03(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess04(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess05(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess06(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess07(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
def CallSubProcess08(pythonCmd):
    stdout_result = CallSubProcess(pythonCmd)
    return stdout_result
#%% Exception handler
 
from safe_schedule import SafeScheduler
scheduler = SafeScheduler()
 
#%% Setting up the functions
 
 
from fn_fetch_cass_mktdata_to4 import cass_mktdata_to4
from fn_fetch_heston_data import fetch_heston_data_to4
from fn_import_strat_params import import_strat_params
from fn_xvv_volsurface_generate import xvv_volsurface_generate, xvv_volsurface_hourly
from fn_ratio_model_to_Qr4 import ratio_model_to4
from fn_calibrationparams_upd import calibrationparams_upd
 
def work(work_type = 'homework'):
    print("Get ready for work! ", work_type)
 
 
 
#%% Scheduling the tasks
 
local_dir = 'C:/synvol/py/logs/'
network_dir = '//na.ad.espeed.com/dfsroot/Vega/Disposable/Jcook/synvol/logs/'
prepared_insert = prepare_scheduledata()
time_now = datetime.datetime.now()
current_mkt_date = str(time_now.date())
str_log_filename = local_dir+'SafeScheduler_' + current_mkt_date.replace('-','') + '.log'
logging.basicConfig(filename=str_log_filename, level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
start_time = str(datetime.datetime.now())
 
logging.warning('SafeScheduler starting: ')
logging.warning(start_time)
 
# === Only load tasks allocated to this computer
df_schedule_tasks = pd.read_excel('scheduler_config.xlsx',sheet_name = 'schedule_items', na_filter = False)
df_schedule_tasks = df_schedule_tasks[df_schedule_tasks.scheduler_name==computer_name]
print('------------------------------------------------------------------------------------')
print('Jobs loaded from scheduler_config.xlsx')
for task in df_schedule_tasks.itertuples(index=False, name='Task'):
    print(task.name_tag,'\t', task.active,'\t',task.every,'\t',task.unit,'\t',task.at,'\t',task.do)
    if(task.active == 1):
        if(task.arg_list != ''):
            getattr(scheduler.every(task.every),task.unit).at(str(task.at)).do(locals()[task.do],task.arg_list)
        else:
            getattr(scheduler.every(task.every),task.unit).at(str(task.at)).do(locals()[task.do])
print('------------------------------------------------------------------------------------')    
print('Jobs loaded into default scheduler')  
for this_job in scheduler.jobs:
    print(this_job)
print('------------------------------------------------------------------------------------')  
print(df_schedule_tasks[['name_tag','active','every','unit','do','depends_tag', 'arg_list']].head(20)) 
 
#%% Load history from 'df_schedule_tasks.csv'
try:
    df_schedule_hist = pd.read_csv( 'df_schedule_tasks.csv', usecols = ['name_tag','every','unit','at','do','depends_tag','arg_list','last'], na_filter = False)
    df_schedule_hist = df_schedule_hist.set_index('do')
 
    if(df_schedule_hist.index.size == df_schedule_tasks.do.count()):
        if(df_schedule_hist.index.to_list() == df_schedule_tasks.do.to_list()):
            have_history = True
    else:
        if(df_schedule_hist.index.size < df_schedule_tasks.do.count()):
            # Add the new task as a row in hist to make job_last run
            for row_ in df_schedule_tasks.itertuples():
 
                if(row_.do not in df_schedule_hist.index):
                    print('++ df_schedule_hist:',row_.do,row_.at,row_.unit)
                    task_row = df_schedule_tasks[df_schedule_tasks.do==row_.do]
                    task_row = task_row.iloc[0].to_dict() # flattens df to one row
                    df_schedule_hist.loc[row_.do] = [task_row['name_tag'],1,task_row['unit'],task_row['at'], task_row['depends_tag'],task_row['arg_list'] ,'']
        if(df_schedule_hist.index.size > df_schedule_tasks.do.count()):
            # Remove the old task from hist
            for row_ in df_schedule_hist.itertuples():
                # row_[0] is the index value for that row
                if(row_[0] not in df_schedule_tasks.do.to_list()):
                    print('-- df_schedule_hist:',row_[0])
                    df_schedule_hist = df_schedule_hist.drop([row_[0]])
       have_history = True
 
except:
    have_history = False
 
#%% run Schedule
# Creating a loop so that the scheduling task keeps on running forever until ctrl-C
run_scheduler = True
while run_scheduler:
    #Checks whether the scheduled task is running or not
    scheduler.run_pending()
    time2 = datetime.datetime.now()
    time2_seconds = time2.second
 
    if(time2_seconds == 0):
        if(have_history):
            job_last = [job_.last_run.strftime('%Y-%m-%d %H:%M:%S') if job_.last_run else df_schedule_hist.loc[job_.job_func.__name__]['last'] for job_ in scheduler.jobs]
        else:
            job_last = [job_.last_run.strftime('%Y-%m-%d %H:%M:%S') if job_.last_run else '' for job_ in scheduler.jobs]
        job_next = [job_.next_run.strftime('%Y-%m-%d %H:%M') if job_.next_run else ''  for job_ in scheduler.jobs]
        job_args = [job_.job_func.args for job_ in scheduler.jobs]
        df_schedule_tasks['last'] = job_last
        df_schedule_tasks['next'] = job_next
        df_schedule_tasks['args'] = job_args
 
        print('time2_seconds',time2_seconds,time2.strftime('%Y-%m-%d %H:%M'),'------------------------------------------')
#        for this_job in scheduler.jobs:
#            print(this_job.job_func.__name__,'\t', ' Every', this_job.interval,this_job.unit[:-1] if this_job.interval == 1 else this_job.unit,weekdays[this_job.next_run.weekday()][:3], 'at', this_job.at_time, '\t', 'last', this_job.last_run, 'next', this_job.next_run, this_job)
        print(df_schedule_tasks[['name_tag','last','next','every','unit','at','do','args']].head(25))
        # scheduler_name ,active ,arg_list ,at_time ,depends_tag ,do_job ,every_freq ,last_run ,name_tag ,next_run ,unit_freq
        if(time2.minute %10 == 0):
            df_schedule_tasks.to_csv('df_schedule_tasks.csv')
            if(cas_fwrite):
                # insert data directly to cassandra table
                df_tbl_scheduler_config = pd.DataFrame()
                df_tbl_scheduler_config['scheduler_name'] = df_schedule_tasks.scheduler_name
                df_tbl_scheduler_config['active'] = df_schedule_tasks.active
                df_tbl_scheduler_config['arg_list'] = df_schedule_tasks.arg_list
                df_tbl_scheduler_config['at_time'] = df_schedule_tasks['at'].apply(str) #- needs to be a string fetch, to avoid conflict w/pandas
                df_tbl_scheduler_config['depends_tag'] = df_schedule_tasks.depends_tag
                df_tbl_scheduler_config['do_job'] = df_schedule_tasks.do
                df_tbl_scheduler_config['every_freq'] = df_schedule_tasks.every
                df_tbl_scheduler_config['last_run'] = job_last # special case to get string
                df_tbl_scheduler_config['name_tag'] = df_schedule_tasks.name_tag
                df_tbl_scheduler_config['next_run'] = job_next
                df_tbl_scheduler_config['unit_freq'] = df_schedule_tasks.unit
                tup_sched_data = [tuple(df_row) for df_row in df_tbl_scheduler_config.values]
                for c_row in tup_sched_data:
                    csession.execute(prepared_insert,c_row)
    time.sleep(1)