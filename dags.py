from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from io import StringIO
#новое
default_args = {
    'owner': 'Lera',
    'start_date': datetime(2024, 7, 25),
}

def fetch_new_clients():
    command = 'curl https://9c579ca6-fee2-41d7-9396-601da1103a3b.selstorage.ru/new_clients.csv'
    output = os.popen(command).read()

    if not output:
        raise Exception("No data retrieved from the source")
    data = StringIO(output)
    df = pd.read_csv(data, delimiter=';')
    
    if df.empty:
        raise Exception("No data in DataFrame. Please check the source CSV file.")
     
    return df

def insert_into_database(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_new_clients')
    
    if df is None or df.empty:
        raise Exception("DataFrame is empty or None.")

    # Переименовываем колонки
    df = df.rename(columns={
        'Date': 'date', 'CustomerId': 'customerid', 'Surname': 'surname', 
        'CreditScore': 'creditscore', 'Geography': 'geography', 'Gender': 'gender',
        'Age': 'age', 'Tenure': 'tenure', 'Balance': 'balance', 
        'NumOfProducts': 'numofproducts', 'HasCrCard': 'hascrcard',
        'IsActiveMember': 'isactivemember', 'EstimatedSalary': 'estimatedsalary', 
        'Exited': 'exited'
    })

    pg_hook = PostgresHook(postgres_conn_id='credit')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO credit_clients (date, customerid, surname, creditscore, geography, gender, age, tenure, balance, numofproducts, hascrcard, isactivemember, estimatedsalary, exited) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customerid) 
    DO UPDATE SET
        date = EXCLUDED.date,
        surname = EXCLUDED.surname,
        creditscore = EXCLUDED.creditscore,
        geography = EXCLUDED.geography,
        gender = EXCLUDED.gender,
        age = EXCLUDED.age,
        tenure = EXCLUDED.tenure,
        balance = EXCLUDED.balance,
        numofproducts = EXCLUDED.numofproducts,
        hascrcard = EXCLUDED.hascrcard,
        isactivemember = EXCLUDED.isactivemember,
        estimatedsalary = EXCLUDED.estimatedsalary,
        exited = EXCLUDED.exited;
    """
    
    for index, row in df.iterrows():
        record = (
            row['date'], row['customerid'], row['surname'], row['creditscore'], row['geography'],
            row['gender'], row['age'], row['tenure'], row['balance'], row['numofproducts'],
            row['hascrcard'], row['isactivemember'], row['estimatedsalary'], row['exited']
        )
        
        cursor.execute(insert_query, record)
    
    connection.commit()
    cursor.close()
    connection.close()

with DAG('lera_daily_clients_data_update', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    fetch_new_clients_task = PythonOperator(
        task_id='fetch_new_clients',
        python_callable=fetch_new_clients,
        dag=dag,
    )

    insert_into_database_task = PythonOperator(
        task_id='insert_into_database',
        python_callable=insert_into_database,
        provide_context=True,
        dag=dag,
    )

fetch_new_clients_task >> insert_into_database_task

