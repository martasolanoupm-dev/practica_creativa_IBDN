# !/usr/bin/env python

import sys, os, re
from os import environ

# Pass date and base path to main() from airflow
def main(base_path):

  # Default to "."
  try: base_path
  except NameError: base_path = "."
  if not base_path:
    base_path = "."

  APP_NAME = "train_spark_mllib_model.py"

  # --- CAMBIO 1: sesión de Spark normal (la config de Iceberg/MinIO viene de spark-defaults.conf) ---
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

  # --- MLflow: conectar y empezar un run (opcional: trazabilidad del entrenamiento) ---
  import mlflow
  import mlflow.spark
  mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
  mlflow.set_experiment("flight_delay_prediction")
  mlflow.start_run()

  from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
  from pyspark.sql.types import StructType, StructField
  from pyspark.sql.functions import udf

  schema = StructType([
    StructField("ArrDelay", DoubleType(), True),
    StructField("CRSArrTime", TimestampType(), True),
    StructField("CRSDepTime", TimestampType(), True),
    StructField("Carrier", StringType(), True),
    StructField("DayOfMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DayOfYear", IntegerType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("FlightDate", DateType(), True),
    StructField("FlightNum", StringType(), True),
    StructField("Origin", StringType(), True),
  ])

  # --- CAMBIO 2: cargar los datos en Iceberg si la tabla no existe (Punto 1) ---
  try:
    spark.table("my_catalog.training.flight_features").first()
    print("Tabla Iceberg ya existe, no recargo los datos")
  except Exception:
    print("Tabla Iceberg no encontrada, cargando datos desde el .bz2...")
    input_path = "{}/data/simple_flight_delay_features.jsonl.bz2".format(base_path)
    raw = spark.read.schema(schema).json(input_path)
    raw.writeTo("my_catalog.training.flight_features").createOrReplace()
    print("Datos cargados en Iceberg: my_catalog.training.flight_features")

  features = spark.table("my_catalog.training.flight_features")
  features.first()

  #
  # Check for nulls in features before using Spark ML
  #
  null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
  cols_with_nulls = filter(lambda x: x[1] > 0, null_counts)
  print(list(cols_with_nulls))

  #
  # Add a Route variable to replace FlightNum
  #
  from pyspark.sql.functions import lit, concat
  features_with_route = features.withColumn(
    'Route',
    concat(
      features.Origin,
      lit('-'),
      features.Dest
    )
  )
  features_with_route.show(6)

  #
  # Bucketize ArrDelay
  #
  from pyspark.ml.feature import Bucketizer
  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )

  # --- CAMBIO 3: guardar en MinIO (Punto 4) ---
  arrival_bucketizer_path = "s3a://practica/models/arrival_bucketizer_2.0.bin"
  arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)

  ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
  ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

  from pyspark.ml.feature import StringIndexer, VectorAssembler

  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(
      inputCol=column,
      outputCol=column + "_index"
    )
    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
    ml_bucketized_features = ml_bucketized_features.drop(column)

    # --- CAMBIO 3 ---
    string_indexer_output_path = "s3a://practica/models/string_indexer_model_{}.bin".format(column)
    string_indexer_model.write().overwrite().save(string_indexer_output_path)

  numeric_columns = [
    "DepDelay", "Distance",
    "DayOfMonth", "DayOfWeek",
    "DayOfYear"]
  index_columns = ["Carrier_index", "Origin_index",
                   "Dest_index", "Route_index"]
  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )
  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

  # --- CAMBIO 3 ---
  vector_assembler_path = "s3a://practica/models/numeric_vector_assembler.bin"
  vector_assembler.write().overwrite().save(vector_assembler_path)

  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)

  final_vectorized_features.show()

  from pyspark.ml.classification import RandomForestClassifier
  rfc = RandomForestClassifier(
    featuresCol="Features_vec",
    labelCol="ArrDelayBucket",
    predictionCol="Prediction",
    maxBins=4657,
    maxMemoryInMB=1024
  )
  model = rfc.fit(final_vectorized_features)

  # --- CAMBIO 3 ---
  model_output_path = "s3a://practica/models/spark_random_forest_classifier.flight_delays.5.0.bin"
  model.write().overwrite().save(model_output_path)

  predictions = model.transform(final_vectorized_features)

  from pyspark.ml.evaluation import MulticlassClassificationEvaluator
  evaluator = MulticlassClassificationEvaluator(
    predictionCol="Prediction",
    labelCol="ArrDelayBucket",
    metricName="accuracy"
  )
  accuracy = evaluator.evaluate(predictions)
  print("Accuracy = {}".format(accuracy))

  predictions.groupBy("Prediction").count().show()

  # --- MLflow: registrar parámetros, métrica y modelo ---
  mlflow.log_param("maxBins", 4657)
  mlflow.log_param("maxMemoryInMB", 1024)
  mlflow.log_metric("accuracy", accuracy)
  mlflow.spark.log_model(model, "random_forest_model")
  mlflow.end_run()

if __name__ == "__main__":
  main(sys.argv[1])
