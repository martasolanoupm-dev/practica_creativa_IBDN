name := "flink_flight_predictor"
version := "1.0"
scalaVersion := "2.12.18"

val flinkVersion = "1.20.0"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-base" % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka" % "3.3.0-1.20",
  "com.datastax.oss" % "java-driver-core" % "4.17.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assembly / assemblyJarName := "flink_flight_predictor.jar"