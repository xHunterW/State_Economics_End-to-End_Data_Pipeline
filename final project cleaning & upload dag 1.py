from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import openpyxl
import psycopg2
import chardet

# define the default arguments
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG
with DAG('data_cleaning_and_upload_dag', default_args=default_args, schedule_interval='@monthly') as dag:

    def load_data(**kwargs):
        # Load Unemployment Data
        
        unemployment_df = pd.read_excel(os.path.join(os.path.dirname(__file__), 'data/UnemploymentReport.xlsx'))
        # Remove the first two rows
        unemployment_df = unemployment_df.iloc[2:]
        # Remove the first column
        unemployment_df.drop(unemployment_df.columns[0], axis=1, inplace=True)
        # Set the first row as the header
        unemployment_df.columns = [unemployment_df.columns[0]] + unemployment_df.iloc[0, 1:].tolist()
        # Drop the first row (now header)
        unemployment_df = unemployment_df.drop(unemployment_df.index[0])
        # Reset the index
        unemployment_df.reset_index(drop=True, inplace=True)
        # Rename columns to match other tables
        unemployment_df.rename(columns={'Unnamed: 1': 'GeoFIPS'}, inplace=True)
        unemployment_df.rename(columns={'Median Household Income (2021)': 'Median_Household_Income_2021'}, inplace=True)
        unemployment_df = unemployment_df.iloc[:-1]  ## Drop Last Row
        unemployment_df['GeoFIPS'] = unemployment_df['GeoFIPS'].apply(
            lambda x: '0'+str(int(x)) if len(str(int(x)))==4 else str(int(x)))

        kwargs['ti'].xcom_push(key='unemployment_df', value=unemployment_df.to_json())
        
        # Load GDP Data
        gdp_df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data/SAGDP2N__ALL_AREAS_1997_2020.csv'))
        gdp_df = gdp_df.iloc[:-4]  # Drop the last four rows
        kwargs['ti'].xcom_push(key='gdp_df', value=gdp_df.to_json())
        
        # Load School Expense Data
        school_expense_df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data/nces330_20.csv'))
        kwargs['ti'].xcom_push(key='school_expense_df', value=school_expense_df.to_json())
        
        # Load Minimum Wage Data
        file_path = os.path.join(os.path.dirname(__file__), 'data/Minimum Wage Data.csv')
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        # Load the CSV file with the detected encoding
        min_wage_df = pd.read_csv(file_path, encoding=result['encoding'])
        kwargs['ti'].xcom_push(key='min_wage_df', value=min_wage_df.to_json())

    def transform_data(**kwargs):
        ti = kwargs['ti']
        
        # Retrieve data
        unemployment_df = pd.read_json(ti.xcom_pull(key='unemployment_df'))
        gdp_df = pd.read_json(ti.xcom_pull(key='gdp_df'))
        school_expense_df = pd.read_json(ti.xcom_pull(key='school_expense_df'))
        min_wage_df = pd.read_json(ti.xcom_pull(key='min_wage_df'))
        
        # Define the primary key check function
        def check_primary_key(df, columns_to_check):
            if df[columns_to_check].isnull().any().any():
                raise ValueError(f"The combination of columns {columns_to_check} contains null values and cannot be a primary key.")
            is_unique = not df.duplicated(subset=columns_to_check).any()
            if is_unique:
                print(f"The combination of columns {columns_to_check} can be a primary key.")
            else:
                duplicate_count = df.duplicated(subset=columns_to_check).sum()
                raise ValueError(f"The combination of columns {columns_to_check} contains {duplicate_count} duplicates and cannot be a primary key.")
        
        # Transform Unemployment Data
        unemployment_df = pd.melt(unemployment_df, id_vars=['GeoFIPS', 'Name','Median_Household_Income_2021'], var_name='Year', value_name='Unemployment Rate')
        unemployment_df.columns=unemployment_df.columns.str.strip()
        
        # Unemployment Tables
        table_Unemployment = unemployment_df[['GeoFIPS','Year','Unemployment Rate']].drop_duplicates()
        table_HouseholdIncome2021 = unemployment_df[['GeoFIPS','Median_Household_Income_2021']].drop_duplicates()
        table_HouseholdIncome2021['Median_Household_Income_2021'] = pd.to_numeric(table_HouseholdIncome2021['Median_Household_Income_2021'].str.replace('$', '').str.replace(',', ''))
        table_location_unemply = unemployment_df[['GeoFIPS','Name']].drop_duplicates()
        # Checking if a combination of columns is unique
        check_primary_key(table_Unemployment, ['GeoFIPS','Year'])
        check_primary_key(table_HouseholdIncome2021, ['GeoFIPS']) 

        # Transform GDP Data
        gdp_df = gdp_df.rename(columns = {'LineCode':'Industry_Code'})
        gdp_df = pd.melt(gdp_df, id_vars=['GeoFIPS', 'GeoName', 'Region', 'TableName', 'Industry_Code','IndustryClassification', 'Description', 'Unit'], var_name='Year', value_name='GDP_In_Millions')
        gdp_df.columns = gdp_df.columns.str.strip()
        gdp_df['GeoFIPS'] = gdp_df['GeoFIPS'].apply(lambda x:x.replace('"', ''))
        
        # GDP Tables
        table_gdp = gdp_df[['GeoFIPS','Year','Industry_Code','GDP_In_Millions']].drop_duplicates()
        table_gdp = table_gdp.rename(columns={'GDP_In_Millions':'GDP'})
        table_gdp['GDP'] = pd.to_numeric(table_gdp['GDP'].str.replace('$', '').str.replace(',', ''), errors='coerce') * 1000000

        table_industry = gdp_df[['Industry_Code','Description']].drop_duplicates()
        table_industry['Description'] = table_industry['Description'].apply(lambda x: x.strip())

        table_location_gdp = gdp_df[['GeoFIPS','GeoName','Region']].drop_duplicates()
        table_location_gdp['GeoFIPS'] = table_location_gdp['GeoFIPS'].apply(lambda x: x.strip())
        table_location_gdp['GeoFIPS'] = table_location_gdp['GeoFIPS'].astype(int)
        table_location = pd.merge(table_location_gdp, table_location_unemply, on='GeoFIPS', how='outer')
        # Fill NaN values in 'GeoName' with values from 'Name'
        table_location['GeoName'] = table_location['GeoName'].fillna(table_location['Name'])
        # Drop the 'Name' column if it's no longer needed
        table_location = table_location.drop(columns=['Name'])
        table_location = table_location.drop_duplicates()
        table_location['GeoName'] = table_location['GeoName'].str.replace('*', '').str.strip()
        table_location['Region'] = pd.to_numeric(table_location['Region'].str.strip(), errors='coerce')

        # Check Primary Keys
        check_primary_key(table_gdp, ['GeoFIPS', 'Year', 'Industry_Code'])
        check_primary_key(table_industry, ['Industry_Code'])
        check_primary_key(table_location, ['GeoFIPS'])
        
        ti.xcom_push(key='table_Unemployment', value=table_Unemployment.to_json())
        ti.xcom_push(key='table_HouseholdIncome2021', value=table_HouseholdIncome2021.to_json())
        ti.xcom_push(key='table_gdp', value=table_gdp.to_json())
        ti.xcom_push(key='table_industry', value=table_industry.to_json())
        ti.xcom_push(key='table_location', value=table_location.to_json())
        
        # Transform School Expense Data
        school_expense_df.columns = school_expense_df.columns.str.strip()
        # Split Tables
        table_school_expense_type = school_expense_df[['Type','Length','Expense']].drop_duplicates()
        table_school_expense_type = table_school_expense_type.reset_index().rename(columns={'index': 'School_Expense_Type_Id'})
        
        table_school_expenses = pd.merge(school_expense_df, table_school_expense_type, on=['Type', 'Length', 'Expense'])
        table_school_expenses = table_school_expenses.rename(columns={'State':'GeoName','Value':'Expense_Amount' })
        table_school_expenses = pd.merge(table_school_expenses, table_location, on=['GeoName'])
        table_school_expenses = table_school_expenses[['Year','GeoFIPS','School_Expense_Type_Id','Expense_Amount']].drop_duplicates()

        table_school_expense_type = table_school_expense_type.rename(columns={'Type':'School_Type', 'Expense':'Expense_Type'})
        
        # Checking if a combination of columns is unique
        check_primary_key(table_school_expense_type, ['School_Expense_Type_Id'])
        check_primary_key(table_school_expenses,['Year','GeoFIPS','School_Expense_Type_Id'])
        
        ti.xcom_push(key='table_school_expense_type', value=table_school_expense_type.to_json())
        ti.xcom_push(key='table_school_expenses', value=table_school_expenses.to_json())
        
        # Transform Minimum Wage Data
        min_wage_df.columns = min_wage_df.columns.str.strip()
        min_wage_df['2020inflationMultiplier'] = min_wage_df['State.Minimum.Wage.2020.Dollars'] / min_wage_df['State.Minimum.Wage']
        min_wage_df = min_wage_df.rename(columns={'State': 'GeoName', 'Department.Of.Labor.Cleaned.Low.Value':'SmallBusinessMinWage','Department.Of.Labor.Cleaned.High.Value':'LargeBusinessMinWage'})
        # Split Tables
        table_state_min_wage = min_wage_df[['Year', 'GeoName', 'State.Minimum.Wage','SmallBusinessMinWage','LargeBusinessMinWage']].drop_duplicates()
        table_state_min_wage = pd.merge(table_state_min_wage, table_location, on=['GeoName'])
        table_state_min_wage = table_state_min_wage[['Year', 'GeoFIPS', 'State.Minimum.Wage','SmallBusinessMinWage','LargeBusinessMinWage']]
        table_state_min_wage = table_state_min_wage.drop_duplicates()
        table_state_min_wage = table_state_min_wage.rename(columns={'State.Minimum.Wage':'State_Min_Wage', 'SmallBusinessMinWage':'Small_Business_Min_Wage', 'LargeBusinessMinWage':'Large_Business_Min_Wage'})

        table_inflation = min_wage_df[['Year','2020inflationMultiplier']].drop_duplicates()
        table_inflation.rename(columns={'2020inflationMultiplier': 'Inflation_Multiplier_2020'}, inplace=True)
        table_inflation = table_inflation.groupby('Year').mean().reset_index()

        table_CPI = min_wage_df[['Year','CPI.Average']].drop_duplicates()
        table_CPI = table_CPI.rename(columns={'CPI.Average':'CPI_Average'})

        table_fed_min_wage = min_wage_df[['Year','Federal.Minimum.Wage']].drop_duplicates()
        table_fed_min_wage = table_fed_min_wage.rename(columns={'Federal.Minimum.Wage':'Fed_Min_Wage'})
        # Checking if a combination of columns is unique
        check_primary_key(table_state_min_wage,['Year','GeoFIPS'])
        check_primary_key(table_inflation,['Year'])
        check_primary_key(table_CPI,['Year'])
        check_primary_key(table_fed_min_wage,['Year'])
        
        ti.xcom_push(key='table_state_min_wage', value=table_state_min_wage.to_json())
        ti.xcom_push(key='table_inflation', value=table_inflation.to_json())
        ti.xcom_push(key='table_CPI', value=table_CPI.to_json())
        ti.xcom_push(key='table_fed_min_wage', value=table_fed_min_wage.to_json())

    def save_data(**kwargs):
        ti = kwargs['ti']
        
        # Retrieve transformed data
        table_Unemployment = pd.read_json(ti.xcom_pull(key='table_Unemployment'))
        table_HouseholdIncome2021 = pd.read_json(ti.xcom_pull(key='table_HouseholdIncome2021'))
        table_gdp = pd.read_json(ti.xcom_pull(key='table_gdp'))
        table_industry = pd.read_json(ti.xcom_pull(key='table_industry'))
        table_location = pd.read_json(ti.xcom_pull(key='table_location'))
        table_school_expense_type = pd.read_json(ti.xcom_pull(key='table_school_expense_type'))
        table_school_expenses = pd.read_json(ti.xcom_pull(key='table_school_expenses'))
        table_state_min_wage = pd.read_json(ti.xcom_pull(key='table_state_min_wage'))
        table_inflation = pd.read_json(ti.xcom_pull(key='table_inflation'))
        table_CPI = pd.read_json(ti.xcom_pull(key='table_CPI'))
        table_fed_min_wage = pd.read_json(ti.xcom_pull(key='table_fed_min_wage'))
        
        # Save to CSV
        table_Unemployment.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_Unemployment.csv'), index=False)
        table_HouseholdIncome2021.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_HouseholdIncome2021.csv'), index=False)
        table_gdp.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_gdp.csv'), index=False)
        table_industry.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_industry.csv'), index=False)
        table_location.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_location.csv'), index=False)
        table_school_expense_type.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_school_expense_type.csv'), index=False)
        table_school_expenses.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_school_expenses.csv'), index=False)
        table_state_min_wage.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_state_min_wage.csv'), index=False)
        table_inflation.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_inflation.csv'), index=False)
        table_CPI.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_CPI.csv'), index=False)
        table_fed_min_wage.to_csv(os.path.join(os.path.dirname(__file__), 'data/table_fed_min_wage.csv'), index=False)


    def upload_data(**kwargs):
        ti = kwargs['ti']

        # Define a function to map pandas dtypes to PostgreSQL data types
        def map_dtype_to_sql(dtype):
            if pd.api.types.is_integer_dtype(dtype):
                return 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                return 'DOUBLE PRECISION'  # Use DOUBLE PRECISION for floating-point numbers
            elif pd.api.types.is_bool_dtype(dtype):
                return 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return 'TIMESTAMP'
            else:
                return 'TEXT'

        # Retrieve transformed data
        table_names = [
            'table_Unemployment',
            'table_HouseholdIncome2021',
            'table_industry',
            'table_location',
            'table_school_expense_type',
            'table_school_expenses',
            'table_state_min_wage',
            'table_inflation',
            'table_CPI',
            'table_fed_min_wage',
            'table_gdp'
        ]

        dataframes = {}
        for table_name in table_names:
            df = pd.read_json(ti.xcom_pull(key=table_name))
            df = df.drop_duplicates()  # Drop duplicates
            df.columns = df.columns.str.replace(' ', '_')  # Replace spaces with underscores in column names
            df.columns = df.columns.str.replace('.', '_')  # Replace periods with underscores in column names
            dataframes[table_name] = df

            # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            'postgres://avnadmin:AVNS_dUt8uqSbJlEOdm6DOlb@project-jh-e494.d.aivencloud.com:22077/defaultdb?sslmode=require')
        cur = conn.cursor()

        # Create schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS final_project")
        conn.commit()

        # Create tables if they do not exist
        for table_name, df in dataframes.items():
            # Generate SQL to create table if it does not exist
            columns_with_types = ', '.join([f'{col} {map_dtype_to_sql(df[col].dtype)}' for col in df.columns])
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS final_project.{table_name} (
                    {columns_with_types}
                );
                """
            cur.execute(create_table_sql)
            conn.commit()

            # Delete all existing records from the table
            delete_sql = f"DELETE FROM final_project.{table_name};"
            cur.execute(delete_sql)
            
            # Commit the transaction
            conn.commit()
    
            # Write the DataFrame to a CSV file
            csv_file_path = os.path.join('/tmp', f'{table_name}.csv')
            df.to_csv(csv_file_path, index=False)

            # Use copy_expert to load the data from the CSV file into the PostgreSQL table
            with open(csv_file_path, 'r') as f:
                copy_sql = f"COPY final_project.{table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                cur.copy_expert(copy_sql, f)
            conn.commit()

            # Remove the CSV file after loading
            os.remove(csv_file_path)

            # Close the database connection
        cur.close()
        conn.close()

# Define tasks
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    dag=dag,
)

upload_data_task = PythonOperator(
    task_id='upload_data',
    python_callable=upload_data,
    dag=dag,
)

# Set task dependencies
load_data_task >> transform_data_task >> save_data_task >> upload_data_task
