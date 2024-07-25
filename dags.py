from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from io import StringIO

default_args = {
    'owner': 'Admin',
    'start_date': datetime(2024, 7, 25)
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
        'Date': 'date', 'CustomerId': 'customer_id', 'Surname': 'surname', 
        'CreditScore': 'credit_score', 'Geography': 'geography', 'Gender': 'gender',
        'Age': 'age', 'Tenure': 'tenure', 'Balance': 'balance', 
        'NumOfProducts': 'num_of_products', 'HasCrCard': 'has_cr_card',
        'IsActiveMember': 'is_active_member', 'EstimatedSalary': 'estimated_salary', 
        'Exited': 'exited'
    })


    # Преобразование значений в логический тип
    df['has_cr_card'] = df['has_cr_card'].astype(bool)
    df['is_active_member'] = df['is_active_member'].astype(bool)
    df['exited'] = df['exited'].astype(bool)

    pg_hook = PostgresHook(postgres_conn_id='psql')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO credit_clients (date, customer_id, surname, credit_score, geography, gender, age, tenure, balance, num_of_products, has_cr_card, is_active_member, estimated_salary, exited) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) 
    DO UPDATE SET
        date = EXCLUDED.date,
        surname = EXCLUDED.surname,
        credit_score = EXCLUDED.credit_score,
        geography = EXCLUDED.geography,
        gender = EXCLUDED.gender,
        age = EXCLUDED.age,
        tenure = EXCLUDED.tenure,
        balance = EXCLUDED.balance,
        num_of_products = EXCLUDED.num_of_products,
        has_cr_card = EXCLUDED.has_cr_card,
        is_active_member = EXCLUDED.is_active_member,
        estimated_salary = EXCLUDED.estimated_salary,
        exited = EXCLUDED.exited;
    """
    
    for index, row in df.iterrows():
        record = (
            row['date'], row['customer_id'], row['surname'], row['credit_score'], row['geography'],
            row['gender'], row['age'], row['tenure'], row['balance'], row['num_of_products'],
            row['has_cr_card'], row['is_active_member'], row['estimated_salary'], row['exited']
        )
        
        cursor.execute(insert_query, record)
    
    connection.commit()
    cursor.close()
    connection.close()



with DAG('daily_clients_data_update', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
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