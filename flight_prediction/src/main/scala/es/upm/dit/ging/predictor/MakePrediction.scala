package es.upm.dit.ging.predictor

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    // 1) Sesión de Spark con acceso a MinIO (S3A) para leer los modelos
    val spark = SparkSession
      .builder
      .appName("FlightDelayPredictor")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .getOrCreate()
    import spark.implicits._

    // 2) Cargar los modelos entrenados desde el lakehouse (MinIO)
    val base_path = "s3a://practica"
    val arrivalBucketizerPath = "%s/models/arrival_bucketizer_2.0.bin".format(base_path)
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns = Seq("Carrier", "Origin", "Dest", "Route")

    val stringIndexerModelPath = columns.map(n => ("%s/models/string_indexer_model_"
      .format(base_path) + "%s.bin".format(n)).toSeq)
    val stringIndexerModel = stringIndexerModelPath.map { n => StringIndexerModel.load(n.toString) }
    val stringIndexerModels = (columns zip stringIndexerModel).toMap

    val vectorAssemblerPath = "%s/models/numeric_vector_assembler.bin".format(base_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    val randomForestModelPath = "%s/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    // 3) Leer en streaming las peticiones desde Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    // 4) Esquema del JSON de cada petición y parseo a columnas
    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))

    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek", "flight.DayOfYear", "flight.DayOfMonth", "flight.Dest",
      "flight.DepDelay", "flight.Timestamp", "flight.FlightDate",
      "flight.Carrier", "flight.UUID", "flight.Distance",
      "flight.Carrier_index", "flight.Origin_index", "flight.Dest_index", "flight.Route_index")

    // 5) Construir la Route, vectorizar y predecir
    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(
        flightFlattenedDf2("Origin"),
        lit('-'),
        flightFlattenedDf2("Dest")
      )
    )

    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    val finalVectorizedFeatures = vectorizedFeatures
      .drop("Carrier_index")
      .drop("Origin_index")
      .drop("Dest_index")
      .drop("Route_index")

    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

    // 6) Por cada lote: escribir en Cassandra y publicar la respuesta en Kafka
    val query = finalPredictions
      .writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val session = CqlSession.builder()
          .addContactPoint(new InetSocketAddress("cassandra", 9042))
          .withLocalDatacenter("datacenter1")
          .build()

        val props = new Properties()
        props.put("bootstrap.servers", "kafka:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val kafkaProducer = new KafkaProducer[String, String](props)

        batchDF.collect().foreach { row =>
          val uuid = row.getAs[String]("UUID")
          val prediction = row.getAs[Double]("Prediction")

          session.execute(
            s"""INSERT INTO agile_data_science.flight_delay_ml_response
               (uuid, origin, dest, route, carrier, dep_delay, distance,
                day_of_week, day_of_year, day_of_month, prediction)
               VALUES (
                '$uuid',
                '${row.getAs[String]("Origin")}',
                '${row.getAs[String]("Dest")}',
                '${row.getAs[String]("Route")}',
                '${row.getAs[String]("Carrier")}',
                ${row.getAs[Double]("DepDelay")},
                ${row.getAs[Double]("Distance")},
                ${row.getAs[Int]("DayOfWeek")},
                ${row.getAs[Int]("DayOfYear")},
                ${row.getAs[Int]("DayOfMonth")},
                $prediction)"""
          )

          val message = s"""{"UUID": "$uuid", "Prediction": $prediction}"""
          kafkaProducer.send(new ProducerRecord[String, String]("flight-delay-ml-response", uuid, message))
        }

        session.close()
        kafkaProducer.close()
      }
      .option("checkpointLocation", "/tmp/cassandra-checkpoint")
      .start()

    // Salida por consola también, para ver las predicciones en el log
    val consoleOutput = finalPredictions.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // 7) Mantener el streaming vivo
    query.awaitTermination()
  }
}
