package es.upm.dit.ging.predictor

import java.net.InetSocketAddress
import scala.collection.JavaConverters._

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.io.Source

/**
 * FlinkPredictor - Job Flink para prediccion de retrasos de vuelos.
 * Sustituye al MakePrediction de Spark. Consume peticiones de Kafka,
 * aplica el Random Forest exportado a JSON y publica el resultado en
 * Cassandra y en un topic de respuesta de Kafka.
 */
object FlinkPredictor {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // --- 1. Fuente Kafka ---
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setTopics("flight-delay-ml-request")
      .setGroupId("flink-predictor")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaStream = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks[String](),
      "kafka-source"
    )

    // --- 2. Prediccion ---
    val predictionStream = kafkaStream
      .map(new PredictFunction())
      .name("random-forest-predict")

    // --- 3. Sink Kafka respuesta ---
    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[String]()
          .setTopic("flight-delay-ml-response")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    predictionStream.sinkTo(kafkaSink).name("kafka-response-sink")

    // --- 4. Sink Cassandra (custom) ---
    predictionStream.addSink(new CassandraCustomSink()).name("cassandra-sink")

    env.execute("Flink Flight Delay Predictor")
  }

  /**
   * MapFunction rica: carga el modelo desde JSON una sola vez y aplica prediccion
   * por cada mensaje.
   */
  class PredictFunction extends RichMapFunction[String, String] {

    @transient private var mapper: ObjectMapper = _
    @transient private var trees: java.util.List[JsonNode] = _
    @transient private var stringIndexers: JsonNode = _

    override def open(parameters: Configuration): Unit = {
      mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      val modelPath = "/opt/flink/data/flink_model.json"
      val jsonStr = Source.fromFile(modelPath).mkString
      val model = mapper.readTree(jsonStr)

      trees = model.get("trees").elements().asScala.toList.asJava
      stringIndexers = model.get("string_indexers")

      println(s"[FlinkPredictor] Modelo cargado: ${trees.size} arboles")
    }

    override def map(value: String): String = {
      try {
        val req = mapper.readTree(value)

        val uuid = req.get("UUID").asText()
        val carrier = req.get("Carrier").asText()
        val origin = req.get("Origin").asText()
        val dest = req.get("Dest").asText()
        val depDelay = req.get("DepDelay").asDouble()
        val distance = req.get("Distance").asDouble()
        val dayOfMonth = req.get("DayOfMonth").asInt()
        val dayOfWeek = req.get("DayOfWeek").asInt()
        val dayOfYear = req.get("DayOfYear").asInt()
        val route = s"$origin-$dest"

        val carrierIdx = indexOrDefault("Carrier", carrier)
        val originIdx  = indexOrDefault("Origin", origin)
        val destIdx    = indexOrDefault("Dest", dest)
        val routeIdx   = indexOrDefault("Route", route)

        val features = Array(
          depDelay, distance,
          dayOfMonth.toDouble, dayOfWeek.toDouble, dayOfYear.toDouble,
          carrierIdx, originIdx, destIdx, routeIdx
        )

        val prediction = predict(features)

        val resp = mapper.createObjectNode()
        resp.put("UUID", uuid)
        resp.put("Origin", origin)
        resp.put("Dest", dest)
        resp.put("Prediction", prediction)
        mapper.writeValueAsString(resp)

      } catch {
        case e: Exception =>
          println(s"[FlinkPredictor] Error procesando mensaje: ${e.getMessage}")
          "{}"
      }
    }

    private def indexOrDefault(indexerName: String, value: String): Double = {
      val idx = stringIndexers.get(indexerName)
      if (idx != null && idx.has(value)) idx.get(value).asDouble()
      else -1.0
    }

    private def predict(features: Array[Double]): Double = {
      val votes = scala.collection.mutable.Map[Double, Int]()
      val it = trees.iterator()
      while (it.hasNext) {
        val tree = it.next()
        val pred = evaluateTree(tree, features)
        votes(pred) = votes.getOrElse(pred, 0) + 1
      }
      votes.maxBy(_._2)._1
    }

    private def evaluateTree(node: JsonNode, features: Array[Double]): Double = {
      if (node.get("is_leaf").asBoolean()) {
        node.get("prediction").asDouble()
      } else {
        val featureIdx = node.get("feature").asInt()
        val featureVal = features(featureIdx)
        val splitType = node.get("split_type").asText()

        val goLeft = if (splitType == "continuous") {
          val threshold = node.get("threshold").asDouble()
          featureVal <= threshold
        } else {
          val cats = node.get("categories_left").elements().asScala.map(_.asDouble()).toSet
          cats.contains(featureVal)
        }

        if (goLeft) evaluateTree(node.get("left"), features)
        else evaluateTree(node.get("right"), features)
      }
    }
  }

  /**
   * Sink propio para Cassandra usando el driver Datastax directamente.
   * Evita el flink-connector-cassandra que da problemas de compatibilidad.
   */
  class CassandraCustomSink extends RichSinkFunction[String] {
    @transient private var session: CqlSession = _
    @transient private var preparedInsert: PreparedStatement = _
    @transient private var mapper: ObjectMapper = _

    override def open(parameters: Configuration): Unit = {
      session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress("cassandra", 9042))
        .withLocalDatacenter("datacenter1")
        .withKeyspace("agile_data_science")
        .build()

      preparedInsert = session.prepare(
        "INSERT INTO flight_delay_ml_response (uuid, origin, dest, prediction) VALUES (?, ?, ?, ?)"
      )

      mapper = new ObjectMapper()
      println("[CassandraCustomSink] Conectado a Cassandra")
    }

    override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
      try {
        val n = mapper.readTree(value)
        val uuid = n.get("UUID").asText()
        val origin = n.get("Origin").asText()
        val dest = n.get("Dest").asText()
        val prediction = n.get("Prediction").asDouble()

        session.execute(preparedInsert.bind(uuid, origin, dest, java.lang.Double.valueOf(prediction)))
      } catch {
        case e: Exception =>
          println(s"[CassandraCustomSink] Error insertando: ${e.getMessage}")
      }
    }

    override def close(): Unit = {
      if (session != null) session.close()
    }
  }
}