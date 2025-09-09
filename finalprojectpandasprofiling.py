

import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from ydata_profiling import ProfileReport





# define the default arguments
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG
with DAG('generate_pandas_reports', default_args=default_args, schedule_interval=None) as dag:
    # Function to load data from PostgreSQL tables
    def load_data_tables(my_param):
        # List of table names
        tables = ['table_Unemployment', 'table_HouseholdIncome2021', 'table_industry', 'table_location', 'table_school_expense_type', 
          'table_school_expenses', 'table_state_min_wage', 'table_inflation', 'table_CPI', 'table_fed_min_wage', 'table_gdp']
        # SQL query to select all data from the specified table
        query = "SELECT * FROM final_project." + tables[my_param]
         # Connect to the PostgreSQL database
        conn = psycopg2.connect('postgres://avnadmin:AVNS_dUt8uqSbJlEOdm6DOlb@project-jh-e494.d.aivencloud.com:22077/defaultdb?sslmode=require')
        # Execute the query and load the data into a pandas DataFrame
        df = pd.read_sql(query, con=conn)
        return df

    # Function to generate pandas profiling reports
    def generate_pandas_reports(my_param):
        # Load data for the specified table
        df = load_data_tables(my_param)
        # List of table names
        tables = ['table_Unemployment', 'table_HouseholdIncome2021', 'table_industry', 'table_location', 'table_school_expense_type', 
          'table_school_expenses', 'table_state_min_wage', 'table_inflation', 'table_CPI', 'table_fed_min_wage', 'table_gdp']
        # Get the table name based on the parameter
        table_name = tables[my_param]
        # Generate a pandas profiling report
        profile = ProfileReport(df)
        # Define the output file name for the report
        table_file = table_name + '_report.html'
        # Save the report to an HTML file
        profile.to_file(table_file)
        return 'Successful exports'


    # Loop to create tasks for each table
    for x in range(11):
        arr = ['a','b','c','d','e','f','g','h','i','j','k']
        char = arr[x]
        # Define task IDs for loading data and generating reports
        string_1 = 'load_data_tables_'+ str(char)
        string_2 = 'generate_pandas_reports_'+ str(char)

        #task to load data from the table
        load_data_task = PythonOperator(
            task_id=string_1,
            python_callable=load_data_tables, 
            op_kwargs={"my_param":x})

        # task to generate a pandas profiling report
        process_data_task = PythonOperator(
            task_id=string_2,
            python_callable=generate_pandas_reports,
            op_kwargs={"my_param":x}
        )

        load_data_task >> process_data_task


    # PLEASE NOTE: See Instruction document for files to turn in.
