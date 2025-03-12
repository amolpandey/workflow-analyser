from dotenv import dotenv_values
import requests as rq
import json
from datetime import datetime 
from flatten_json import flatten
import pandas as pd
from datetime import timedelta

def clean_dict_for_invalid_values(raw_dict: dict) -> dict:
    raw_copy = raw_dict.copy()
    for k in raw_dict.keys():
            if isinstance(raw_dict[k], dict):
                if len(raw_dict[k].keys()) == 0:
                    del raw_copy[k]
            if isinstance(raw_dict[k], str):
                if len(raw_dict[k]) == 0:
                    del raw_copy[k]
    return raw_copy

def get_workflow_job_defitnion(host, token):
    # Import the JSON data from the databricks about the Workflow Jobs
    result_json = []
    next_page_token = 'intialization'
    while next_page_token is not None:
        get_jobs_url = f'{host}/api/2.2/jobs/list?expand_tasks=true{"&page_token=" + next_page_token if next_page_token != "intialization" else ""}'
        print(get_jobs_url)
        response = rq.get(get_jobs_url, headers={'Authorization': f'Bearer {token}'})
        jresp = json.loads(response.content)
        next_page_token = jresp.get('next_page_token')
        result_json.append(jresp)

    # Interpet the JSON data and collate to form  a dataframe in the downstream steps 
    result = []
    result_task = []
    for pg in result_json:
        for jbinfo in pg.get('jobs'):
            dobj = {}
            dobj_task = {}

            for k in jbinfo.keys():
                if k not in ['settings','created_time']:
                    dobj[k] = jbinfo[k]
                elif k == 'created_time':
                    dobj[k] = datetime.fromtimestamp(int(jbinfo[k]) / 1000)

            for k in dobj.keys():
                dobj_task[k] = dobj[k]

            settings_json = jbinfo.get('settings')

            flatten_settings_data = flatten(settings_json)
            for k in flatten_settings_data.keys():
                if not k.startswith('tasks_'): # Ignore the tasks data
                    dobj[k] = flatten_settings_data[k]
                else:
                    dobj_task[k] = flatten_settings_data[k]

            result.append(dobj)
            result_task.append(dobj_task)

    # Final result cleanup for the empty dict or strings
    result_clean = []
    for obj in result:
        obj_clean = clean_dict_for_invalid_values(obj)
        result_clean.append(obj_clean)

    result_task_clean = []
    for obj in result_task:
        obj_clean = clean_dict_for_invalid_values(obj)
        result_task_clean.append(obj_clean)

    return result_clean , result_task_clean

def get_workflow_job_history(host, token, job_id):
    # Import the JSON data from the databricks about the Workflow Jobs
    result_json = []
    next_page_token = 'intialization'
    while next_page_token is not None:
        get_jobs_url = f'{host}/api/2.2/jobs/runs/list?job_id={job_id}&expand_tasks=true{"&page_token=" + next_page_token if next_page_token != "intialization" else ""}'
        print(get_jobs_url)
        response = rq.get(get_jobs_url, headers={'Authorization': f'Bearer {token}'})
        jresp = json.loads(response.content)
        next_page_token = jresp.get('next_page_token')
        result_json.append(jresp)

    run_history = []
    for jobj in result_json:
        if 'runs' in jobj:
            for robj in jobj['runs']:
                fjobj = flatten(robj, root_keys_to_ignore=['tasks','job_clusters'])
                fjobj['start_time'] = datetime.fromtimestamp(int(fjobj['start_time']) / 1000)
                fjobj['run_duration'] = timedelta(milliseconds=int(fjobj['run_duration']))
                run_history.append(fjobj)
    
    rpdf = None
    if len(run_history) > 0:
        rpdf = pd.DataFrame(run_history)

    return rpdf

if __name__ == '__main__':
    config = dotenv_values('.env')
    databricks_host = config['databricks_host']
    databricks_token = config['databricks_user_token']

    # Import Job Task Defition
    job_def_dict, job_task_def_dict =  get_workflow_job_defitnion(databricks_host, databricks_token)
    
    job_def_df = pd.DataFrame(job_def_dict)
    job_def_df.to_parquet('data/job_def.parquet', index=False)

    job_task_def_df = pd.DataFrame(job_task_def_dict)
    job_task_def_df.to_parquet('data/job_task_def.parquet', index=False)

    # Import the Job History
    history_df = None
    for jb in job_def_dict:
        print('Importing history for ' + str(jb['job_id']) + ' - ' + jb['name'])
        
        run_history_df = get_workflow_job_history(databricks_host, databricks_token,  int(jb['job_id']))

        if run_history_df is not None:
            if history_df is None:
                history_df = run_history_df
            else:
                history_df = pd.concat([run_history_df, history_df], ignore_index=True)
    
    if history_df is not None:
        history_df.to_parquet('data/job_history.parquet', index=False)