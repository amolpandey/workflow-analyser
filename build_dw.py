import pandas as pd 
from cron_descriptor import get_description

job_def = pd.read_parquet('data/job_def.parquet')
job_task_def = pd.read_parquet('data/job_task_def.parquet')
job_history = pd.read_parquet('data/job_history.parquet')

# Prepare the table job.parquet
columns_filter = [c for c in job_def.columns if not c.startswith(('job_clusters_', 'tags_'))]
job_df = job_def[columns_filter]
job_df['schedule_quartz_cron_expression_description'] = job_df['schedule_quartz_cron_expression'].apply(lambda x: get_description(x) if x is not None else None)
job_df.to_parquet('dw/job.parquet', index=False)

# Prepare the table tag.parquet
columns_filter = [c for c in job_def.columns if c.startswith(('job_id', 'tags_'))]
tag_df = pd.melt(job_def[columns_filter] , id_vars=['job_id'] , value_vars=[x for x in columns_filter if x != 'job_id'], var_name='tag_key', value_name='tag_value') 
tag_df = tag_df[tag_df['tag_value'].notnull()]
tag_df.to_parquet('dw/tag.parquet', index=False)

# Prepare the table cluster.parquet
columns_filter = [c for c in job_def.columns if c.startswith(('job_id', 'job_clusters_') )]
clst_df = pd.melt(job_def[columns_filter] , id_vars=['job_id'] , value_vars=[x for x in columns_filter if x != 'job_id'], var_name='cluster_key', value_name='cluster_value') 
clst_df = clst_df[clst_df['cluster_value'].notnull()]
clst_df['cluster_name'] = clst_df['cluster_key'].apply(lambda x: '_'.join(x.split('_',3)[:3]))
clst_df['cluster_key'] = clst_df['cluster_key'].apply(lambda x: x.split('_',3)[3])
clst_df = clst_df[['job_id','cluster_name','cluster_key','cluster_value']]
clst_df['cluster_value'] = clst_df['cluster_value'].astype(str)
clst_df.to_parquet('dw/cluster.parquet', index=False)

# Prepare the table task.parquet
columns_filter = [c for c in job_task_def.columns if c.startswith(('job_id', 'tasks_') )]
tasks_df = pd.melt(job_task_def[columns_filter] , id_vars=['job_id'] , value_vars=[x for x in columns_filter if x != 'job_id'], var_name='task_key', value_name='task_value') 
tasks_df = tasks_df[tasks_df['task_value'].notnull()]
tasks_df['task_name'] = tasks_df['task_key'].apply(lambda x: '_'.join(x.split('_',2)[:2]))
tasks_df['task_key'] = tasks_df['task_key'].apply(lambda x: x.split('_',2)[2])
tasks_df = tasks_df[['job_id','task_name','task_key','task_value']]
tasks_df['task_value'] = tasks_df['task_value'].astype(str)
tasks_df.to_parquet('dw/task.parquet', index=False)

# Prepare the table history.parquet
job_history.to_parquet('dw/history.parquet', index=False)
