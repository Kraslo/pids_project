from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Función para procesar el archivo CSV
def process_csv():
    # Ruta al archivo CSV que subiste al servidor
    csv_file_path = '/home/pids_user/rows.csv'  # Asegúrate de que la ruta sea correcta

    print(f'Procesando el archivo: {csv_file_path}')

    # Leer las primeras 30 filas del archivo CSV
    df = pd.read_csv(csv_file_path, nrows=30)

    # Aquí puedes realizar el procesamiento que necesites. Por ahora, imprime las primeras filas.
    print(df.head())

    # Si deseas guardar el CSV procesado, puedes guardarlo en otra ruta
    processed_file_path = '/home/pids_user/processed_rows.csv'
    df.to_csv(processed_file_path, index=False)

# Definir el DAG
with DAG(
    dag_id='process_raws_csv_dag',  # Identificador único para tu DAG
    start_date=datetime(2024, 10, 3),  # La fecha a partir de la cual comenzará a correr
    schedule='@daily',  # Corre una vez al día
    catchup=False,  # Evita correr el DAG para fechas anteriores si no estaba activo
) as dag:

    # Crear la tarea para procesar el CSV
    process_csv_task = PythonOperator(
        task_id='process_raws_csv',  # Identificador único para la tarea
        python_callable=process_csv
    )

process_csv()
