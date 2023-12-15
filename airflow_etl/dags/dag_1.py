


import os
import pandas as pd
import csv
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow import Dataset
from airflow.contrib.hooks.mongo_hook import MongoHook
default_args = {
    'owner': 'Igor'
}



PATH = '/home/igor/airflow_etl/tmp'
FILENAME = 'tiktok_google_play_reviews.csv'
FULLPATH = PATH + "/" + FILENAME
DATASETSPATH = '/home/igor/airflow_etl/datasets/dag_1_result.csv'

DAG1_DATASET = Dataset("file:/" + DATASETSPATH)

def determine_branch():    
    if os.stat(FULLPATH).st_size == 0:
        return 'file_is_empty'
    else:
        return 'Transform_Data.read_csv_file'
        
def read_csv_file():
    df = pd.read_csv(FULLPATH,header=0)
    print(df)
    return df.to_json(orient = 'records',default_handler=str)
        
def replace_null_values(ti):
    json_data = ti.xcom_pull(task_ids='Transform_Data.read_csv_file')
    df = pd.read_json(json_data)
    df['at'] = df['at'].fillna(value='1678-01-01 23:18:30')
    df['content'] = df['content'].fillna(value='-')
    df['score'] = df['score'].fillna(value=0)
    print(df)
    return df.to_json(orient = 'records',default_handler=str)

def sort_by_created_date(ti):
    json_data = ti.xcom_pull(task_ids='Transform_Data.replace_null_values_task_id')
    df = pd.read_json(json_data)
    df = df.sort_values(by=['at'])
    print(df)
    return df.to_json(orient = 'records',default_handler=str)

def drop_spec_symb_date(ti):
    json_data = ti.xcom_pull(task_ids='Transform_Data.sort_by_created_date')
    df = pd.read_json(json_data)
    df = df.replace({'content': r'[^0-9a-zA-Z:,\s]+'},  {'content': ''}, regex=True)
    print(df)
    return df.to_json(orient = 'records',default_handler=str)


def saving_to_csv(ti):
    clean_data = ti.xcom_pull(task_ids='Transform_Data.drop_spec_symb_date')
    df = pd.read_json(clean_data)

    df.to_csv(DATASETSPATH, index=False,encoding='utf-8')
    

with DAG(dag_id='etl_for_task_7',
        description='Running a pipeline with a Postgres hook',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once'
) as dag_1:

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = FULLPATH,
        poke_interval = 10,
        timeout = 60 * 10,
            dag = dag_1
    )

    file_empty_branch = BranchPythonOperator(
        task_id='determine_branch_task',
        python_callable=determine_branch,
            dag = dag_1
    )

    file_empty = BashOperator(
        task_id = 'file_is_empty',
        bash_command = 'echo !!! FILE IS EMPTY !!!',
        dag = dag_1
    )

    saving_to_csv_task = PythonOperator(
        task_id = 'saving_to_csv',
        python_callable = saving_to_csv,
        outlets=[DAG1_DATASET],
        dag = dag_1
    )

    drop_input_csv_task = BashOperator(
        task_id = 'drop_input_csv',
        bash_command = 'rm {0}'.format(FULLPATH),
        dag = dag_1
    )

    with TaskGroup('Transform_Data') as transform_data:        
        read_csv_file_task = PythonOperator(
            task_id = 'read_csv_file',
            python_callable = read_csv_file,
            dag = dag_1
        )
        replace_null_values_task = PythonOperator(
            task_id = 'replace_null_values_task_id',
            python_callable = replace_null_values,
            dag = dag_1
        )
        sort_by_created_date_task = PythonOperator(
            task_id = 'sort_by_created_date',
            python_callable = sort_by_created_date,
            dag = dag_1
        )
        drop_spec_symb_date_task = PythonOperator(
            task_id = 'drop_spec_symb_date',
            python_callable = drop_spec_symb_date,
            dag = dag_1
        )
        
        read_csv_file_task >> replace_null_values_task >> sort_by_created_date_task >> drop_spec_symb_date_task
        

    checking_for_file >> file_empty_branch 
    file_empty_branch >> Label('File is empty') >> file_empty
    file_empty_branch >> Label('File is NOT empty') >> transform_data
    transform_data >> Label('Save dataset') >> saving_to_csv_task >> Label('Drop input file') >>drop_input_csv_task


def read_dataset_csv_file():
    df = pd.read_csv(DATASETSPATH,header=0)
    print(df)
    return df.to_json(orient = 'records',default_handler=str)

def read_data_for_mongo():
    mongo_hook = MongoHook(conn_id='mongo_conn')
    df = pd.read_csv(DATASETSPATH,header=0,usecols=["content", "score", "at"],encoding='utf-8')
    data = df.to_json(orient = 'records',default_handler=str)
    print(data)
    return data

def clear_data_to_mongo():
    mongo_hook = MongoHook(conn_id='mongo_conn')
    del_cnt = mongo_hook.delete_many(mongo_collection='collection_etl', filter_doc={})
    print(del_cnt)

def insert_data_to_mongo(ti):
    mongo_hook = MongoHook(conn_id='mongo_conn')
    data = json.loads(ti.xcom_pull(task_ids='read_data_for_mongo'))
    print(data)
    for item in data:
        mongo_hook.insert_one(mongo_collection='collection_etl', doc=item)
    #x = mongo_hook.find(mongo_collection='collection_etl', query={})


def get_data_from_mongo():
    mongo_hook = MongoHook(conn_id='mongo_conn')
    print(mongo_hook)
    data = mongo_hook.find(mongo_collection='collection_etl', query={})
    print('1!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    df = pd.DataFrame(list(data))
    print(df)
    res = df.to_json(orient = 'records',default_handler=str)
    return res

def top_5_friquently_comments(ti):
    json_data = ti.xcom_pull(task_ids='get_data_from_mongo')
    df = pd.read_json(json_data)    
    df = (
        df
        .groupby(df['content'])
        ['content']
        .agg(
            cnt  = 'count',
        )
        .sort_values(['cnt'], ascending=False)
        .head(5)
    )
    df.to_csv('/home/igor/airflow_etl/output/top_5_friquently_comments.csv',encoding='utf-8')
    print(df)

def content_smaller_then_5(ti):
    json_data = ti.xcom_pull(task_ids='get_data_from_mongo')
    df = pd.read_json(json_data)    
    df = df[df['content'].str.len() < 5]
    df.to_csv('/home/igor/airflow_etl/output/content_smaller_then_5.csv',encoding='utf-8')
    print(df)

def avg_by_at_field(ti):
    json_data = ti.xcom_pull(task_ids='get_data_from_mongo')
    df = pd.read_json(json_data)   
    print(df) 
    df['at'] = pd.to_datetime(df["at"].str[0:10])
    print(df) 
    
    df = (
        df
        .groupby(df['at'])
        ['score']
        .agg(
            my_avg  = 'mean',
        )
    )
    
    df.to_csv('/home/igor/airflow_etl/output/avg_by_at_field.csv',encoding='utf-8')
    print(df)

with DAG(dag_id='etl_for_task_7_next_1',
        description='Running a pipeline with a Postgres hook',
        default_args=default_args,
        start_date=days_ago(1),
        schedule=[DAG1_DATASET]
) as dag_2:
    read_data_for_mongo_task = PythonOperator(
        task_id = 'read_data_for_mongo',
        python_callable = read_data_for_mongo,
        dag = dag_2
    )   


    clear_data_to_mongo_task = PythonOperator(
        task_id = 'clear_data_to_mongo',
        python_callable = clear_data_to_mongo,
        dag = dag_2
    ) 
    insert_data_to_mongo_task = PythonOperator(
        task_id = 'insert_data_to_mongo',
        python_callable = insert_data_to_mongo,
        dag = dag_2
    )   

    get_data_from_mongo_task = PythonOperator(
        task_id = 'get_data_from_mongo',
        python_callable = get_data_from_mongo,
        dag = dag_2
    ) 
    content_smaller_then_5_task = PythonOperator(
        task_id = 'content_smaller_then_5',
        python_callable = content_smaller_then_5,
        dag = dag_2
    ) 

    top_5_friquently_comments_task = PythonOperator(
        task_id = 'top_5_friquently_comments',
        python_callable = top_5_friquently_comments,
        dag = dag_2
    )  

    avg_by_at_field_task = PythonOperator(
        task_id = 'avg_by_at_field',
        python_callable = avg_by_at_field,
        dag = dag_2
    ) 
    
    drop_dataset_csv_task = BashOperator(
        task_id = 'drop_dataset_csv',
        bash_command = 'rm {0}'.format(DATASETSPATH),
        dag = dag_2
    )
    
    read_data_for_mongo_task >> clear_data_to_mongo_task >>insert_data_to_mongo_task >>\
    get_data_from_mongo_task >> [top_5_friquently_comments_task,content_smaller_then_5_task,avg_by_at_field_task] >>\
    drop_dataset_csv_task
    
    