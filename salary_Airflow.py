import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

RAW_DATA_PATH = r"C:\Users\acer\Documents\R2DE\mini-project\data\Salary_Data.csv"
CLEAN_DATA_PATH = r"C:\Users\acer\Documents\R2DE\mini-project\data\Salary_Data_Cleaned.csv"
DB_PATH = r"C:\Users\acer\Documents\R2DE\mini-project\data\salary_insights.db"

def extract_salary_data():
    df = pd.read_csv(RAW_DATA_PATH)
    df.to_csv(CLEAN_DATA_PATH, index=False)
    print("Extract สำเร็จ")

def transform_salary_data():
    df = pd.read_csv(CLEAN_DATA_PATH)
    
    df = df.dropna() 
    
    df['Education Level'] = df['Education Level'].str.replace("Degree", "").str.strip()
    
    df['is_senior'] = df['Years of Experience'] > 5
    
    df.to_csv(CLEAN_DATA_PATH, index=False)
    print("Transform สำเร็จ: ล้างข้อมูลและเพิ่มคอลัมน์ is_senior เรียบร้อย")

def load_to_sqlite():
    df = pd.read_csv(CLEAN_DATA_PATH)
    conn = sqlite3.connect(DB_PATH)
    
    df.to_sql('salary_table', conn, if_exists='replace', index=False)
    conn.close()
    print("Load สำเร็จ")

with DAG(
    'salary_data_etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract', python_callable=extract_salary_data)
    t2 = PythonOperator(task_id='transform', python_callable=transform_salary_data)
    t3 = PythonOperator(task_id='load', python_callable=load_to_sqlite)

    t1 >> t2 >> t3
