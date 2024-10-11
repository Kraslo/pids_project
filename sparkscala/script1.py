import datetime
import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, mean
from pyspark.sql.types import IntegerType, FloatType
from kafka import KafkaConsumer
from pymongo import MongoClient

# Inicializar sesión de Spark
def create_spark_session():
    return SparkSession.builder \
        .appName("CSV_Processing_Spark") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def process_and_store_metrics_spark(file_path, mongo_url='mongodb://mongotainer:27017'):
    spark = create_spark_session()

    # Conectar a MongoDB
    client = MongoClient(mongo_url)
    db = client['mi_base_de_datos']
    collection = db['mi_coleccion']
    
    # Cargar archivo CSV en un DataFrame de Spark
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Filtrar por la fecha actual
    df = df.withColumn("pickup_date", col("tpep_pickup_datetime").cast("date"))
    today = datetime.datetime.today().date()
    # df_today = df.filter(col("pickup_date") == today)
    # df_today = df.filter(col("pickup_date") == col("pickup_date")) # HARDCODED FOR DEBUG PURPORSES!!!!!
    df_today = df
    print(df.head())

    if df_today.count() == 0:
        print("No hay datos para el día actual.")
        return

    # Insertar datos procesados en MongoDB
    json_data = df_today.toPandas().to_dict(orient="records")
    if json_data:
        collection.insert_many(json_data)
        print(f"Datos insertados correctamente en MongoDB para {file_path}")
    else:
        print(f"No hay datos para insertar en MongoDB desde {file_path}")

    # Generar y almacenar métricas
    generate_and_store_metrics(df_today, db)

def generate_and_store_metrics(df, db):
    # Calcular métricas usando PySpark
    metrics = {}
    try:
      metrics['total_registros'] = df.count()
      metrics['distancia_promedio'] = df.agg(mean("trip_distance")).first()[0]
      metrics['total_pasajeros'] = df.agg(spark_sum("passenger_count")).first()[0]
      metrics['total_propinas'] = df.agg(spark_sum("tip_amount")).first()[0]
      metrics['viajes_por_tipo_pago'] = df.groupBy("payment_type").count().rdd.collectAsMap()
      metrics['total_ingresos'] = df.agg(spark_sum("total_amount")).first()[0]
      metrics['viajes_por_ratecode'] = df.groupBy("RatecodeID").count().rdd.collectAsMap()

      # metrics['fecha_procesada'] = df.select("pickup_date").first()[0].strftime("%Y-%m-%d")
      pickup_date = df.select("pickup_date").first()[0]
      if pickup_date is None:
            metrics['fecha_procesada'] = "Fecha no disponible"
      else:
            metrics['fecha_procesada'] = pickup_date.strftime("%Y-%m-%d")
      db['metricas'].insert_one(metrics)
      print(f"Métricas guardadas en MongoDB para el día {metrics['fecha_procesada']}")

    except Exception as e:
      print(f'Excepción ocurrida: {str(e)}')
    # Guardar métricas en MongoDB
      print(f'PROCESO DE GUARDADO EN MONGO FALLADO')

# Consumidor de Kafka para recibir paths de fragmentos CSV
def consume_and_process_kafka_messages(topic='spark_input_topic', mongo_url='mongodb://mongotainer:27017'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['pidskafka:9092'],
        auto_offset_reset='earliest',
        group_id='spark-group'
    )

    for message in consumer:
        file_path = message.value.decode('utf-8')
        print(f"Procesando archivo CSV en {file_path}")
        process_and_store_metrics_spark(file_path, mongo_url)

if __name__ == "__main__":
    # Comienza a escuchar el topic de Kafka
    consume_and_process_kafka_messages()





