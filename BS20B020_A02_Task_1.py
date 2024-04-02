# Import necessary libraries
import os
import random
import zipfile
import datetime
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Get Airflow variables
target_year = Variable.get('target_year')
num_selected_files = Variable.get('num_selected_files')
output_path = Variable.get('output_path')

# Define functions for data processing
def extract_and_select_files():
    # Parse HTML and select random CSV files
    with open('/tmp/data/index.html', 'r') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
        csv_links = [link['href'] for link in soup.find_all('a') if link['href'].endswith('.csv')]
        selected_files = random.sample(csv_links, int(num_selected_files))
    
    # Write selected file names to output text file
    with open('/tmp/data/output.txt', 'w') as out_file:
        out_file.write('\n'.join(str(i) for i in selected_files))
        return selected_files
    
def compress_files():
    # Zip CSV files in the dataset folder
    with zipfile.ZipFile(f'/tmp/data/{target_year}_data.zip', 'w') as zip_file:
        for foldername, subfolders, filenames in os.walk('/tmp/data/dataset/'):
            for file_name in filenames:
                zip_file.write(os.path.join(foldername, file_name), arcname=file_name)

def move_archive():
    # Move the zip archive to the specified path
    os.system(f"mv /tmp/data/{target_year}_data.zip {output_path}")

# Define default arguments for the DAG
default_dag_args = {
    'owner': 'malavika',
    'start_date': datetime.datetime(2024, 3, 10),
    'retries': 0,
}

# Define the DAG with specified ID and default arguments
data_fetch_dag = DAG(
    dag_id='DataFetch_Pipeline',
    default_args=default_dag_args,
)

# Define tasks for the DAG
fetch_html_task = BashOperator(
    task_id='fetch_html_page',
    bash_command=f"mkdir -p /tmp/data/dataset && wget -O /tmp/data/index.html 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{target_year}'",
    dag=data_fetch_dag,
)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=extract_and_select_files,
    dag=data_fetch_dag,
)

download_data_task = BashOperator(
    task_id='download_data',
    bash_command=f"""
    while read line; do
        wget -P /tmp/data/dataset/ "https://www.ncei.noaa.gov/data/local-climatological-data/access/{target_year}/$line"
    done < /tmp/data/output.txt
    """,
    dag=data_fetch_dag,
)

zip_data_task = PythonOperator(
    task_id='zip_data',
    python_callable=compress_files,
    dag=data_fetch_dag,
)

move_data_task = PythonOperator(
    task_id='move_data',
    python_callable=move_archive,
    dag=data_fetch_dag,
)

# Set task dependencies
fetch_html_task >> select_files_task >> download_data_task >> zip_data_task >> move_data_task
