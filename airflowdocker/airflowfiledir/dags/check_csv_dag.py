import os
import time
import subprocess
import paramiko
from kafka import KafkaProducer
import glob

import csv

print(f"Directorio actual: {os.getcwd()}")
# Ruta del archivo CSV que vamos a chequear
CSV_FILE_PATH = "/app/"
# Host y credenciales del servidor SFTP (nodo maestro de Spark)
SFTP_HOST = "spark-master"  # Cambiar por el hostname del nodo maestro de Spark
SFTP_PORT = 22  # Puerto SFTP, generalmente es 22
SFTP_USERNAME = "sftpuser"  # Cambiar por el nombre de usuario del servidor SFTP
SFTP_PASSWORD = "sftppassword"  # Cambiar por la contraseña del servidor SFTP
SFTP_WORKING_DIR = "/uploads/"  # Directorio en Spark para subir los archivos
# Directorio base para kafka
REMOTE_WORKING_DIR = f"/home/{SFTP_USERNAME}"

# Función para dividir el archivo en fragmentos más pequeños
def split_file(input_file, chunk_size_mb=100):
    """Divide el archivo CSV en trozos de tamaño especificado en MB y devuelve las rutas de los fragmentos"""
    chunk_size_bytes = chunk_size_mb * 1024 * 1024  # Convertir MB a Bytes
    output_files = []

    # Abrimos el archivo original en modo binario
    with open(input_file, 'rb') as f:
        chunk_count = 0
        while True:
            chunk = f.read(chunk_size_bytes)
            if not chunk:
                break
            print(f"Tamaño del fragmento {chunk_count + 1}: {len(chunk)} bytes")

            # Crear un nuevo archivo para el fragmento
            chunk_filename = f"{input_file}.part{chunk_count + 1}"
            output_files.append(chunk_filename)
            with open(chunk_filename, 'wb') as chunk_file:
                chunk_file.write(chunk)
            chunk_count += 1
    print(f'DEBUG OUTPUT FILES: {output_files}')
    return output_files

def split_file(input_file, chunk_size_mb=100):
    """Divide el archivo CSV en trozos de tamaño especificado en MB, agregando los encabezados a cada fragmento."""
    chunk_size_bytes = chunk_size_mb * 1024 * 1024  # Convertir MB a Bytes
    output_files = []

    # Abrimos el archivo original en modo texto para obtener los encabezados
    with open(input_file, 'r', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Leer solo la primera línea que contiene los headers
        headers_line = ','.join(headers) + '\n'  # Convertimos los headers en una cadena de texto

    # Abrimos el archivo original en modo binario para dividir el contenido en chunks
    with open(input_file, 'rb') as f:
        chunk_count = 0

        # Saltar la línea de encabezado del archivo binario para que no se incluya en los chunks de datos
        f.readline()  # Leer y descartar la primera línea de encabezado en modo binario

        while True:
            chunk = f.read(chunk_size_bytes)
            if not chunk:
                break

            # Mostrar información del tamaño del fragmento
            print(f"Tamaño del fragmento {chunk_count + 1}: {len(chunk)} bytes")

            # Crear un nuevo archivo para el fragmento
            chunk_filename = f"{input_file}.part{chunk_count + 1}"
            output_files.append(chunk_filename)

            # Escribimos los headers primero y luego el fragmento de datos
            with open(chunk_filename, 'w', newline='') as chunk_file:
                chunk_file.write(headers_line)  # Agregar encabezados
                chunk_file.write(chunk.decode('utf-8'))  # Agregar el fragmento de datos (convertido a texto)

            chunk_count += 1

    print(f'DEBUG OUTPUT FILES: {output_files}')
    return output_files

# Función para enviar el archivo a través de SFTP
def send_csv_file():
    """Envía los archivos .csv divididos en fragmentos pequeños a Spark a través de SFTP y manda un mensaje a Kafka"""
    kafka_topic = "spark_input_topic"  # Tópico Kafka para enviar la ruta
    print(f"Directorio actual: {os.getcwd()}")
    # Obtener todos los archivos .csv en el directorio especificado
    csv_files = glob.glob(os.path.join(CSV_FILE_PATH, "*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No se encontraron archivos .csv en {CSV_FILE_PATH}")

    # Conexión SFTP usando paramiko
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Para cada archivo CSV encontrado
    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            print(f"El archivo {csv_file} no existe")
            continue

        # Dividir el archivo en fragmentos de 100MB
        chunks = split_file(csv_file, chunk_size_mb=100)
        print(f'DEBUG {chunks}')
        # Para cada fragmento del archivo
        for chunk in chunks:
            # Subir el fragmento a través de SFTP al directorio de Spark
            remote_path = f"{SFTP_WORKING_DIR}{os.path.basename(chunk)}"
            print(remote_path)
            print(chunks)
            kafka_path = f"{REMOTE_WORKING_DIR}{remote_path}"
            # kafka_caller = lambda x, y: send_kafka_message(kafka_topic, kafka_path)
            try:
                sftp.put(chunk, remote_path) # , callback=kafka_caller)
                print(f"Fragmento {chunk} subido a SFTP: {remote_path}")
            except Exception as e:
                print(f"Error al subir el fragmento {chunk}: {str(e)}")
                continue

            # Enviar la ruta del fragmento a Kafka
            # kafka_path = f"{REMOTE_WORKING_DIR}{remote_path}"
            send_kafka_message(kafka_topic, kafka_path)

            # Eliminar el fragmento local después de subirlo
            os.remove(chunk)

    # Cerrar la conexión SFTP
    sftp.close()
    transport.close()

def send_kafka_message(topic, message):
    """Envía un mensaje a Kafka con la ruta del archivo"""
    producer = KafkaProducer(bootstrap_servers=['pidskafka:9092'])  # Cambiar según el puerto de Kafka

    future = producer.send(topic, message.encode('utf-8'))
    result = future.get(timeout=10)
    print(f"Mensaje enviado a Kafka al tópico {topic}: {message}")

    # Cerrar el productor
    producer.close()

def check_csv_update():
    """Función que chequeará si el archivo CSV ha sido modificado."""
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"{CSV_FILE_PATH} no existe")

    # Obtener la hora de la última modificación del archivo
    last_modified_time = os.path.getmtime(CSV_FILE_PATH)

    # Hora actual
    current_time = time.time()

    # Si el archivo fue modificado en las últimas 24 horas, se considera nuevo
    if current_time - last_modified_time < 86400:  # 86400 segundos = 24 horas
        return True
    return False

def process_csv():
    """Llama al script de Python para procesar el CSV e insertar los datos en MongoDB."""
    result = subprocess.run(["python", SCRIPT_PATH], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error al procesar el CSV: {result.stderr}")
    print(f"Resultado del procesamiento: {result.stdout}")

# Definir el DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG(
    dag_id='check_and_process_csv_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Chequeo diario
    catchup=False
) as dag:

    check_csv = PythonOperator(
        task_id='check_csv',
        python_callable=check_csv_update
    )

    send_data = PythonOperator(
        task_id='send_data',
        python_callable=send_csv_file
    )

    # Definir la secuencia de tareas
    check_csv >> send_data
