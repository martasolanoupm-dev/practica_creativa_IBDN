#!/bin/bash

IP=$(curl -s ifconfig.me || echo "TU_IP")

echo ""
echo "==> Levantando servicios"
docker compose up -d
echo "[OK]"

echo ""
echo "==> Esperando Cassandra"
until [ "$(docker inspect -f '{{.State.Health.Status}}' cassandra 2>/dev/null)" = "healthy" ]; do
    echo -n "."
    sleep 3
done
echo " [OK]"

echo ""
echo "==> Esperando Kafka"
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do
    echo -n "."
    sleep 3
done
echo " [OK]"

echo ""
echo "==> Esperando MinIO"
until docker exec minio curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo " [OK]"

echo ""
echo "==> Bucket MinIO"
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1
if docker exec minio mc ls local/practica > /dev/null 2>&1; then
    echo "[OK] ya existe"
else
    docker exec minio mc mb local/practica
    echo "[OK] creado"
fi

echo ""
echo "==> Topics Kafka"
for TOPIC in flight-delay-ml-request flight-delay-ml-response; do
    if docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null | grep -q "^${TOPIC}$"; then
        echo "[OK] ${TOPIC} ya existe"
    else
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic ${TOPIC} --partitions 1 --replication-factor 1 > /dev/null
        echo "[OK] ${TOPIC} creado"
    fi
done

echo ""
echo "==> Cassandra keyspace y tablas"
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};" > /dev/null 2>&1
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (origin text, dest text, distance int, PRIMARY KEY (origin, dest));" > /dev/null 2>&1
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, carrier text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, dest text, distance double, flight_date date, origin text, prediction double, route text, timestamp timestamp);" > /dev/null 2>&1
echo "[OK]"

echo ""
echo "==> Distancias en Cassandra"
COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null | grep -oE '[0-9]+' | tail -1)
COUNT=${COUNT:-0}
if [ "$COUNT" -ge 4696 ]; then
    echo "[OK] $COUNT registros"
else
    echo "[AVISO] solo $COUNT, cargalas con NiFi en http://${IP}:8091"
fi

echo ""
echo "==> Modelo en MinIO"
if docker exec minio mc ls local/practica/models/spark_random_forest_classifier.flight_delays.5.0.bin > /dev/null 2>&1; then
    echo "[OK] entrenado"
else
    echo "[AVISO] no encontrado, entrenar con Airflow o perfil train"
fi

echo ""
echo "==> Flink como predictor"
if [ "$(docker inspect -f '{{.State.Running}}' predictor 2>/dev/null)" = "true" ]; then
    docker stop predictor > /dev/null
    echo "[OK] predictor Spark parado"
fi

JOB_RUNNING=$(docker exec flink-jobmanager flink list 2>/dev/null | grep -c "Flink Flight Delay Predictor")
JOB_RUNNING=${JOB_RUNNING:-0}
if [ "$JOB_RUNNING" -gt 0 ]; then
    echo "[OK] job ya desplegado"
else
    if docker exec flink-jobmanager test -f /opt/flink/job/target/scala-2.12/flink_flight_predictor.jar; then
        docker exec flink-jobmanager flink run -d -c es.upm.dit.ging.predictor.FlinkPredictor /opt/flink/job/target/scala-2.12/flink_flight_predictor.jar > /dev/null 2>&1
        echo "[OK] job desplegado"
    else
        echo "[AVISO] JAR no encontrado, compilar con sbt assembly"
    fi
fi

echo ""
echo "============================================================="
echo "  Sistema listo"
echo "============================================================="
echo ""
echo "  Flask:       http://${IP}:5001/flights/delays/predict_kafka"
echo "  MinIO:       http://${IP}:9001    (minioadmin/minioadmin)"
echo "  Spark:       http://${IP}:8080"
echo "  Flink:       http://${IP}:8092"
echo "  MLflow:      http://${IP}:5000"
echo "  Airflow:     http://${IP}:8090    (admin/admin)"
echo "  NiFi:        http://${IP}:8091/nifi"
echo "  Grafana:     http://${IP}:3000    (admin/admin)"
echo "  Prometheus:  http://${IP}:9090"
echo ""