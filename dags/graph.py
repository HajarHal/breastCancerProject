from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd
import os
from pyvis.network import Network
from datetime import datetime

def create_db_connection(**kwargs):
    connection_string = 'postgresql://airflow:airflow@postgres/airflow'
    kwargs['ti'].xcom_push(key='db_connection_string', value=connection_string)

def generate_graphs_from_table(table_name, cancer_types, output_subfolder, **kwargs):
    """Generate graphs based on data from the specified PostgreSQL table."""
    connection_string = kwargs['ti'].xcom_pull(key='db_connection_string')
    engine = create_engine(connection_string)

    base_output_dir = '/opt/airflow/dags/flask_app/assets/graph'  

    df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)

    for cancer_type, folder_name in cancer_types.items():
        output_dir = os.path.join(base_output_dir, folder_name)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        categories = df[df['Cancer_Type'] == cancer_type]['Category'].unique()

        for category in categories:
            df_category = df[(df['Category'] == category) & (df['Cancer_Type'] == cancer_type)]
            net = Network(notebook=False)

            for _, row in df_category.iterrows():
                net.add_node(row['SUBJECT_NAME'], title=row['SUBJECT_NAME'], size=15, color='lightblue')
                net.add_node(row['Cancer_Type'], title=row['Cancer_Type'], size=15, color='lightblue')
                net.add_edge(
                    row['SUBJECT_NAME'], 
                    row['Cancer_Type'], 
                    color='gray'
                )
            
            file_path = os.path.join(output_dir, f'{category}.html')
            net.write_html(file_path)
            print(f"Graph for category '{category}' under '{cancer_type}' saved at: {file_path}")

def generate_causes_graphs(**kwargs):
    """Generate causes graphs."""
    cancer_types = {
        'Female Breast Cancer': 'causes_female',
        'Male Breast Cancer': 'causes_male',
        'Recurrent Breast Cancer': 'causes_reccu'
    }
    generate_graphs_from_table('causes_table', cancer_types, 'causes', **kwargs)

def generate_prevents_graphs(**kwargs):
    """Generate prevention graphs."""
    cancer_types = {
        'Female Breast Cancer': 'prevents_female',
        'Male Breast Cancer': 'prevents_male',
        'Recurrent Breast Cancer': 'prevents_reccu'
    }
    generate_graphs_from_table('preventions_table', cancer_types, 'prevents', **kwargs)

def generate_treats_graphs(**kwargs):
    """Generate treatment graphs."""
    cancer_types = {
        'Female Breast Cancer': 'treats_female',
        'Male Breast Cancer': 'treats_male',
        'Recurrent Breast Cancer': 'treats_reccu'
    }
    generate_graphs_from_table('treatments_table', cancer_types, 'treats', **kwargs)

def generate_diagnoses_graphs(**kwargs):
    """Generate diagnosis graphs."""
    cancer_types = {
        'Female Breast Cancer': 'diagnoses_female',
        'Male Breast Cancer': 'diagnoses_male',
        'Recurrent Breast Cancer': 'diagnoses_reccu'
    }
    generate_graphs_from_table('diagnoses_table', cancer_types, 'diagnoses', **kwargs)

with DAG(
    'graph_dag',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 10, 1)},
    schedule_interval='@daily',
    catchup=False
) as dag:
    create_db_connection_task = PythonOperator(
        task_id='create_db_connection',
        python_callable=create_db_connection,
        provide_context=True,  
    )

    generate_causes_graphs_task = PythonOperator(
        task_id='generate_causes_graphs',
        python_callable=generate_causes_graphs,
        provide_context=True,
    )
    
    generate_prevents_graphs_task = PythonOperator(
        task_id='generate_prevents_graphs',
        python_callable=generate_prevents_graphs,
        provide_context=True,
    )
    
    generate_treats_graphs_task = PythonOperator(
        task_id='generate_treats_graphs',
        python_callable=generate_treats_graphs,
        provide_context=True,
    )
    
    generate_diagnoses_graphs_task = PythonOperator(
        task_id='generate_diagnoses_graphs',
        python_callable=generate_diagnoses_graphs,
        provide_context=True,
    )

# Set up task dependencies
create_db_connection_task >> generate_causes_graphs_task >> generate_prevents_graphs_task >> generate_treats_graphs_task >> generate_diagnoses_graphs_task
