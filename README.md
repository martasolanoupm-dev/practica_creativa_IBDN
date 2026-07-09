# Práctica Creativa IBDN — Flight Predictor

**Autora:** Marta Solano · GISD · ETSIT-UPM
**Convocatoria:** Extraordinaria 2025-2026
**Repositorio:** github.com/martasolanoupm-dev/practica_creativa_IBDN

Sistema Big Data de predicción de retrasos de vuelos en tiempo real. Todo dockerizado, desplegado en Google Cloud.

---

## Índice

1. [Preparación del entorno](#1-preparación-del-entorno)
2. [Arranque rápido](#2-arranque-rápido)
3. [Arranque manual paso a paso](#3-arranque-manual-paso-a-paso)
4. [Punto 1 — Datos en Iceberg sobre MinIO](#4-punto-1--datos-en-iceberg-sobre-minio)
5. [Punto 2 — Distancias en Cassandra con NiFi](#5-punto-2--distancias-en-cassandra-con-nifi)
6. [Punto 3 — Predicción Kafka + Cassandra + WebSockets](#6-punto-3--predicción-kafka--cassandra--websockets)
7. [Punto 4 — Todo dockerizado](#7-punto-4--todo-dockerizado)
8. [Punto 5 — Entrenamiento con Airflow y MLflow](#8-punto-5--entrenamiento-con-airflow-y-mlflow)
9. [Opcional — Apache Flink como predictor](#9-opcional--apache-flink-como-predictor)
10. [Opcional — Observabilidad con Prometheus y Grafana](#10-opcional--observabilidad-con-prometheus-y-grafana)
11. [Puertos y URLs](#11-puertos-y-urls)

---

## 1. Preparación del entorno

### VM en Google Cloud

- Instancia: `practica-ibdn` (e2-standard-16, 16 vCPU, 64 GB RAM).
- Zona: `europe-west1-b`.
- Sistema: Ubuntu 24.04.
- Docker y Docker Compose ya instalados.

### Firewall

Abrir en GCloud (regla `practica-ports`) los siguientes puertos TCP:
### Conexión desde local

Usar VS Code con Remote-SSH. La IP externa de la VM cambia en cada arranque; hay que actualizar el HostName en la configuración SSH cada vez.

---

## 2. Arranque rápido

Desde la raíz del proyecto en la VM:

```bash
./start_all.sh
```

Este script hace todo:
- Levanta los ~17 servicios con `docker compose up -d`.
- Espera a que Cassandra, Kafka y MinIO estén healthy.
- Crea el bucket `practica` en MinIO si no existe.
- Crea los topics de Kafka.
- Crea el keyspace y las tablas de Cassandra.
- Comprueba que las distancias y el modelo estén cargados.
- Para el predictor de Spark y despliega el job de Flink.
- Imprime las URLs finales.

Al terminar sale un mensaje "Sistema listo" con todas las webs a mano.

---

## 3. Arranque manual paso a paso

Si prefieres control total o algo falla:

```bash
# Levantar el sistema completo
docker compose up -d

# Esperar ~30 segundos y ver que está todo
docker ps
```

Debes ver ~17 contenedores "Up", con Cassandra y Kafka marcados como `healthy`.

Servicios que se levantan:
- `minio`, `cassandra`, `kafka` (almacenamiento y mensajería).
- `spark-master`, `spark-worker-1`, `spark-worker-2` (cluster Spark).
- `flink-jobmanager`, `flink-taskmanager-1`, `flink-taskmanager-2` (cluster Flink).
- `flask`, `predictor` (aplicación web y predictor original).
- `nifi` (carga de distancias).
- `airflow`, `mlflow` (orquestación y trazabilidad).
- `prometheus`, `grafana`, `node-exporter`, `kafka-exporter` (observabilidad).

---

## 4. Punto 1 — Datos en Iceberg sobre MinIO

### Cómo comprobarlo

```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc ls local/practica/warehouse/training/flight_features/data/
docker exec minio mc ls local/practica/models/
```

Salida esperada:
- En `data/`: dos archivos `.parquet` (3.8 MiB + 415 KiB).
- En `models/`: siete carpetas (bucketizer, 4 string_indexer_model, vector_assembler, random_forest_classifier).

### Cómo verlo en la web

Abrir `http://<IP>:9001` (login `minioadmin/minioadmin`) y navegar a:
Verás dos subcarpetas: `data/` (parquet con los datos) y `metadata/` (manifiestos Iceberg en JSON y Avro).

### Qué demuestra

Los datos de entrenamiento no están en discos locales, sino como tabla Iceberg en el lakehouse. Los modelos entrenados también viven ahí, versionados por MinIO.

---

## 5. Punto 2 — Distancias en Cassandra con NiFi

### Cómo comprobarlo

```bash
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;"
docker exec cassandra cqlsh -e "SELECT * FROM agile_data_science.origin_dest_distances LIMIT 5;"
```

Salida esperada: COUNT = 4696, y 5 filas del tipo `EUG | BOI | 351`.

### Cómo se cargaron (con NiFi, no con script)

Abrir `http://<IP>:8091/nifi` y ver el flujo montado en el canvas:
Cada procesador hace:
- **GetFile**: lee `origin_dest_distances.jsonl` de `/opt/nifi/data`.
- **SplitText**: parte el fichero en 4696 líneas individuales.
- **ReplaceText**: transforma cada línea JSON en un `INSERT INTO ... VALUES (...)` con una regex.
- **PutCassandraQL**: ejecuta el INSERT contra `cassandra:9042`.

### Cómo relanzarlo si vaciaras la tabla

En la interfaz de NiFi, seleccionar las 4 cajitas y darles al play. En cuanto `GetFile` marca `Out: 1`, pararlo (evita duplicados en bucle).

---

## 6. Punto 3 — Predicción Kafka + Cassandra + WebSockets

### Cómo hacer una predicción en vivo

Abrir en el navegador: `http://<IP>:5001/flights/delays/predict_kafka`

Rellenar los campos (por defecto ATL → SFO) y pulsar **Submit**.

En ~1 segundo debe aparecer una etiqueta con la predicción, tipo:
- "Early (15+ Minutes Early)"
- "Slightly Late (0-30 Minute Delay)"
- etc.

### Cómo verificar que se guardó en Cassandra

```bash
docker exec cassandra cqlsh -e "SELECT uuid, origin, dest, prediction FROM agile_data_science.flight_delay_ml_response;" | tail -5
```

La última fila debe tener un UUID recién generado.

### Cómo demostrar los WebSockets con rooms

Abrir **dos pestañas** del navegador con el mismo formulario. Pulsar Submit **solo en una**. La predicción aparece únicamente en esa pestaña; la otra queda vacía.

Esto demuestra que Flask emite el resultado con `socketio.emit('prediction', data, room=UUID)`, no con broadcast.

---

## 7. Punto 4 — Todo dockerizado

Todo el sistema se levanta con un único comando desde la raíz:

```bash
docker compose up -d
```

El `docker-compose.yml` define todos los servicios con sus imágenes, volúmenes persistentes, dependencias y variables de entorno. Nada se instala en el host de la VM.

Para bajar el sistema:
```bash
docker compose down
```

Los volúmenes (datos de Cassandra, MinIO, MLflow, Airflow, NiFi, Grafana, Prometheus) persisten entre `down`/`up`.

---

## 8. Punto 5 — Entrenamiento con Airflow y MLflow

### Cómo lanzar un reentrenamiento

**Paso 1 — parar el predictor de Spark** (retiene los cores del cluster):
```bash
docker stop predictor
```

**Paso 2 — abrir Airflow:** `http://<IP>:8090`, login `admin/admin`.

**Paso 3** — activar el DAG `flight_delay_retraining` con el interruptor a la izquierda del nombre.

**Paso 4** — dispararlo con el botón play (▶) → "Trigger DAG".

**Paso 5** — esperar 1-2 minutos. La tarea `retrain_model` debe pasar a `success` (verde).

**Paso 6 — rearrancar el predictor** (opcional si vas a usar Flink):
```bash
docker start predictor
```

### Cómo comprobar que MLflow registró el run

Abrir `http://<IP>:5000` → experimento `flight_delay_prediction` → verás un run nuevo con:
- Parámetros: `maxBins=4657`, `maxMemoryInMB=1024`.
- Métrica: `accuracy ≈ 0.587`.
- Modelo Spark guardado como artifact.

### Qué demuestra

El DAG de Airflow ejecuta `spark-submit` contra el cluster (2 workers), entrena distribuido, guarda el modelo en MinIO y registra el run en MLflow. La cadena completa: **Airflow → Spark distribuido → MLflow**.

---

## 9. Opcional — Apache Flink como predictor

En la extraordinaria, Flink sustituye a Spark en la parte de predicción.

### Cómo generar el JSON del modelo

Flink no lee modelos serializados por Spark MLlib. Hay que exportarlos primero a JSON:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /practica/resources/export_model_for_flink.py /practica
```

Genera `data/flink_model.json` (~1.7 MB) con los 20 árboles, los splits del bucketizer y los 4 string indexers.

### Cómo compilar el JAR de Flink

```bash
docker run --rm \
  -v ~/practica_creativa/flink_prediction:/proyecto -w /proyecto \
  hseeberger/scala-sbt:17.0.2_1.6.2_2.12.15 sbt assembly
```

Genera `flink_prediction/target/scala-2.12/flink_flight_predictor.jar` (~38 MB).

### Cómo desplegar el job en el cluster

```bash
docker stop predictor

docker exec flink-jobmanager flink run -d \
  -c es.upm.dit.ging.predictor.FlinkPredictor \
  /opt/flink/job/target/scala-2.12/flink_flight_predictor.jar
```

Debe salir un mensaje "Job has been submitted with JobID xxx".

### Cómo comprobar que corre

Abrir `http://<IP>:8092` → debe verse:
- 1 Running Job "Flink Flight Delay Predictor".
- Task Managers: 2.
- Available Task Slots: 4.
- Parallelism: 2.

Y en los logs de los TaskManagers:
```bash
docker logs flink-taskmanager-1 2>&1 | grep "Modelo cargado" | tail -3
```

Debe salir: `[FlinkPredictor] Modelo cargado: 20 arboles`.

### Cómo verificar predicción real

Hacer una predicción en la web y comprobar en Cassandra:
```bash
docker exec cassandra cqlsh -e "SELECT uuid, origin, dest, prediction FROM agile_data_science.flight_delay_ml_response;" | tail -3
```

La última fila es la predicción calculada por Flink.

---

## 10. Opcional — Observabilidad con Prometheus y Grafana

### Prometheus

Abrir `http://<IP>:9090/targets`. Deben verse **3 targets UP** en verde:
- `prometheus` (métricas propias).
- `node` (node-exporter, métricas de la VM).
- `kafka` (kafka-exporter, métricas de Kafka).

Si alguno sale `DOWN`, esperar ~30 segundos y recargar.

### Grafana

Abrir `http://<IP>:3000` (login `admin/admin`, Skip la pantalla de cambio de contraseña).

**Añadir Prometheus como data source** (solo la primera vez):
1. Connections → Data sources → Add data source → Prometheus.
2. Prometheus server URL: `http://prometheus:9090`.
3. Save & test → debe salir un banner verde.

**Importar los dashboards** (solo la primera vez):
1. Dashboards → New → Import.
2. Meter el ID `1860` → Load → seleccionar `prometheus` como datasource → Import. Es el **Node Exporter Full** (CPU, RAM, disco, red).
3. Repetir con el ID `7589`. Es el **Kafka Exporter Overview** (mensajes por segundo, lag, particiones).

### Qué se ve

- **Node Exporter Full**: CPU busy en tiempo real, RAM usada, uso de disco, tráfico de red, uptime, número de cores.
- **Kafka Exporter Overview**: mensajes por segundo por topic (`flight-delay-ml-request` y `flight-delay-ml-response`), lag por consumer group, particiones activas.

---

## 11. Puertos y URLs

| Servicio | Puerto | URL | Credenciales |
|---|---|---|---|
| Flask (web usuario) | 5001 | `http://<IP>:5001/flights/delays/predict_kafka` | — |
| MinIO (lakehouse) | 9001 | `http://<IP>:9001` | minioadmin / minioadmin |
| Spark Master | 8080 | `http://<IP>:8080` | — |
| Flink JobManager | 8092 | `http://<IP>:8092` | — |
| MLflow | 5000 | `http://<IP>:5000` | — |
| Airflow | 8090 | `http://<IP>:8090` | admin / admin |
| NiFi | 8091 | `http://<IP>:8091/nifi` | — |
| Grafana | 3000 | `http://<IP>:3000` | admin / admin |
| Prometheus | 9090 | `http://<IP>:9090` | — |

---

