import sys, os, re, json, uuid, threading
from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room

# Configuración y utilidades (get_flight_distance lee de Cassandra)
import config
import predict_utils

# Productor de Kafka para publicar las peticiones de predicción
from kafka import KafkaProducer

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
PREDICTION_TOPIC = 'flight-delay-ml-request'


@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
  """Página del formulario de predicción (WebSockets)"""
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'}
  ]
  return render_template('flight_delays_predict_kafka.html', form_config=form_config)


@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  """Recibe el formulario, construye la petición y la publica en Kafka"""
  api_field_type_map = {
    "DepDelay": float, "Carrier": str, "FlightDate": str,
    "Dest": str, "FlightNum": str, "Origin": str
  }
  api_form_values = {}
  for name, ftype in api_field_type_map.items():
    api_form_values[name] = request.form.get(name, type=ftype)

  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value

  # Distancia desde Cassandra (Punto 2). get_flight_distance ignora el primer argumento.
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    None, api_form_values['Origin'], api_form_values['Dest']
  )

  # La fecha se convierte en DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(api_form_values['FlightDate'])
  for k, v in date_features_dict.items():
    prediction_features[k] = v

  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()

  # Campos de índice (placeholders; el modelo los gestiona con handleInvalid)
  prediction_features['Carrier_index'] = 0.0
  prediction_features['Origin_index'] = 0.0
  prediction_features['Dest_index'] = 0.0
  prediction_features['Route_index'] = 0.0

  # UUID único: identifica la predicción y será el nombre de la sala WebSocket
  unique_id = str(uuid.uuid4())
  prediction_features['UUID'] = unique_id

  producer.send(PREDICTION_TOPIC, json.dumps(prediction_features).encode())

  return json.dumps({"status": "OK", "id": unique_id})


# --- WebSockets con rooms ---
@socketio.on('join')
def on_join(data):
  """El navegador se une a la sala con su UUID para recibir solo su respuesta"""
  join_room(data['uuid'])


def kafka_consumer_thread():
  """Escucha las respuestas en Kafka y las envía a la sala del UUID correspondiente"""
  from kafka import KafkaConsumer
  consumer = KafkaConsumer(
    'flight-delay-ml-response',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    group_id='flask-web'
  )
  for message in consumer:
    data = json.loads(message.value.decode())
    socketio.emit('prediction', data, room=data['UUID'])


if __name__ == "__main__":
  socketio.start_background_task(kafka_consumer_thread)
  socketio.run(app, host="0.0.0.0", port=5001, allow_unsafe_werkzeug=True)