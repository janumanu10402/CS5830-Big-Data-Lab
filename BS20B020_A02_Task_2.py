# Importing necessary libraries
import os
import random
import shutil
import requests
from zipfile import ZipFile
from datetime import datetime, timedelta
import logging
from bs4 import BeautifulSoup
from ast import literal_eval as make_tuple

import apache_beam as beam
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

# Function to fetch data from the API
def fetch_weather_data(**context):
    year = 2012
    base_url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'

    # Make a request to the API
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find("table").find_all("tr")[2:-2]

    # Select random files
    selected_files = [rows[random.randint(0, len(rows))].find_all("td")[0].text for _ in range(12)]

    # Write binary data to the local directory
    for filename in selected_files:
        file_url = base_url + filename
        response = requests.get(file_url)
        open(filename, 'wb').write(response.content)

    # Zip the files
    with ZipFile('/root/airflow/DAGS/weather.zip', 'w') as zip_file:
        for filename in selected_files:
            zip_file.write(filename)

# Function to unzip files
def unzip_files(**context):
    with ZipFile("/root/airflow/DAGS/weather.zip", 'r') as zip_object:
        zip_object.extractall(path="/root/airflow/DAGS/files")

# DAG definition for the first task
dag_fetch_data = DAG(
    dag_id="fetch_weather_data",
    schedule_interval="@daily",
    default_args={
        "owner": "data_fetching",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "start_date": datetime(2024, 3, 3),
    },
    catchup=False
)

# Task to fetch weather data
fetch_weather_data_task = PythonOperator(
    task_id="fetch_weather_data",
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag_fetch_data
)

# Task to unzip files
unzip_files_task = PythonOperator(
    task_id="unzip_files",
    python_callable=unzip_files,
    provide_context=True,
    dag=dag_fetch_data
)

# Function to parse CSV data
def parse_csv_data(data):
    fields = data.split('","')
    fields[0] = fields[0].strip('"')
    fields[-1] = fields[-1].strip('"')
    return list(fields)

# Beam function to extract and filter fields
class ExtractAndFilterFields(beam.DoFn):
    def __init__(self, required_fields, **kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i, header in enumerate(headers_csv):
            if 'hourly' in header.lower():
                for j in required_fields:
                    if j.lower() in header.lower():
                        self.required_fields.append(i)

        self.headers = {header: index for index, header in enumerate(headers_csv)}

    def process(self, element):
        headers = self.headers
        latitude = element[headers['LATITUDE']]
        longitude = element[headers['LONGITUDE']]
        data = [element[i] for i in self.required_fields]
        if latitude != 'LATITUDE':
            yield ((latitude, longitude), data)

# Beam function to run Beam for processing CSV files
def process_csv_data(**kwargs):
    required_fields = ["LATITUDE", "LONGITUDE", "HourlyDryBulbTemperature"]
    os.makedirs('/root/airflow/DAGS/files/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(parse_csv_data)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a: (a[0][0], a[0][1], a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/result.txt')

# Function to extract fields with month
class ExtractFieldsWithMonth(beam.DoFn):
    def __init__(self, required_fields, **kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i, header in enumerate(headers_csv):
            if 'hourly' in header.lower():
                for j in required_fields:
                    if j.lower() in header.lower():
                        self.required_fields.append(i)

        self.headers = {header: index for index, header in enumerate(headers_csv)}

    def process(self, element):
        headers = self.headers
        latitude = element[headers['LATITUDE']]
        longitude = element[headers['LONGITUDE']]
        data = [element[i] for i in self.required_fields]
        if latitude != 'LATITUDE':
            measure_time = datetime.strptime(element[headers['DATE']], '%Y-%m-%dT%H:%M:%S')
            month_format = "%Y-%m"
            month = measure_time.strftime(month_format)
            yield ((month, latitude, longitude), data)

# Beam function to compute averages
def compute_averages(data):
    value_data = np.array(data[1])
    value_data_shape = value_data.shape
    value_data = pd.to_numeric(value_data.flatten(), errors='coerce', downcast='float')
    value_data = np.reshape(value_data, value_data_shape)
    masked_data = np.ma.masked_array(value_data, np.isnan(value_data))
    result = np.ma.average(masked_data, axis=0)
    result = list(result.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(result)
    return ((data[0][1], data[0][2]), result)

# Beam function to use Beam to compute the averages
def compute_monthly_averages(**kwargs):
    required_fields = ["LATITUDE", "LONGITUDE", "HourlyDryBulbTemperature"]
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/result.txt*')
            | 'PreprocessParse' >> beam.Map(lambda a: make_tuple(a.replace('nan', 'None')))
            | 'ExtractFieldsWithMonth' >> beam.ParDo(ExtractFieldsWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_averages(data))
            | 'CombineTupleWithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a: (a[0][0], a[0][1], a[1]))
        )

        result | 'WriteAveragesToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/results/averages.txt')

# Beam function for aggregated computation
class Aggregated(beam.CombineFn):
    def __init__(self, required_fields, **kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i, header in enumerate(headers_csv):
            if 'hourly' in header.lower():
                for j in required_fields:
                    if j.lower() in header.lower():
                        self.required_fields.append(i)

    def create_accumulator(self):
        return []

    def add_input(self, accumulator, element):
        accumulator_dict = {key: value for key, value in accumulator}
        data = element[2]
        value_data = np.array(data)
        value_data_shape = value_data.shape
        value_data = pd.to_numeric(value_data.flatten(), errors='coerce', downcast='float')
        value_data = np.reshape(value_data, value_data_shape)
        masked_data = np.ma.masked_array(value_data, np.isnan(value_data))
        result = np.ma.average(masked_data, axis=0)
        result = list(result.filled(np.nan))
        for ind, i in enumerate(self.required_fields):
            accumulator_dict[i] = accumulator_dict.get(i, []) + [(element[0], element[1], result[ind])]

        return list(accumulator_dict.items())

    def merge_accumulators(self, accumulators):
        merged_dict = {}
        for a in accumulators:
            a_dict = {key: value for key, value in a}
            for i in self.required_fields:
                merged_dict[i] = merged_dict.get(i, []) + a_dict.get(i, [])

        return list(merged_dict.items())

    def extract_output(self, accumulator):
        return accumulator

# Function to plot geomaps
def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1], dtype='float')
    d1 = np.array(data, dtype='float')

    world = gpd.read_file(gpd.datasets.get_path('naturalearth'))

    data = gpd.GeoDataFrame({
        values[0]: d1[:, 2]
    }, geometry=gpd.points_from_xy(*d1[:, (1, 0)].T))

    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=100, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/root/airflow/DAGS/files/results/plots', exist_ok=True)
    plt.savefig(f'/root/airflow/DAGS/files/results/plots/{values[0]}_heatmap_plot.png')

# Beam function to create plots
def create_heatmap_visualization(**kwargs):
    required_fields = ["LATITUDE", "LONGITUDE", "HourlyDryBulbTemperature"]
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/results/averages.txt*')
            | 'PreprocessParse' >> beam.Map(lambda a: make_tuple(a.replace('nan', 'None')))
            | 'GlobalAggregation' >> beam.CombineGlobally(Aggregated(required_fields=required_fields))
            | 'FlatMap' >> beam.FlatMap(lambda a: a)
            | 'PlotGeomaps' >> beam.Map(plot_geomaps)
        )

# Function to delete CSV files
def delete_csv_files(**kwargs):
    shutil.rmtree('/root/airflow/DAGS/files')

# DAG definition for the second task
dag_data_analysis = DAG(
    dag_id="data_analysis",
    schedule_interval="@daily",
    default_args={
        "owner": "second_task",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1),
    },
    catchup=False
)

# File sensor to wait for the presence of the weather.zip file
wait_for_file = FileSensor(
    task_id='wait_for_file',
    mode="poke",
    poke_interval=5,
    timeout=5,
    filepath="/root/airflow/DAGS/weather.zip",
    dag=dag_data_analysis,
    fs_conn_id="fs_default",
)

# Task to unzip files
unzip_files_task = PythonOperator(
    task_id="unzip_files",
    python_callable=unzip_files,
    provide_context=True,
    dag=dag_data_analysis
)

# Task to process CSV files
process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_data,
    dag=dag_data_analysis,
)

# Task to compute monthly averages
compute_monthly_avg_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_averages,
    dag=dag_data_analysis,
)

# Task to create heatmap visualizations
create_heatmap_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    dag=dag_data_analysis,
)

# Task to delete CSV files
delete_csv_files_task = PythonOperator(
    task_id='delete_csv_files',
    python_callable=delete_csv_files,
    dag=dag_data_analysis,
)

# Define task dependencies
wait_for_file >> unzip_files_task >> process_csv_files_task >> compute_monthly_avg_task >> create_heatmap_task >> delete_csv_files_task
