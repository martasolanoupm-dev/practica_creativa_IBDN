// Conectar al servidor por WebSocket
var socket = io();

// Cuando llega una prediccion para nuestra sala, mostrarla
socket.on('prediction', function(data) {
  renderPage(data);
});

// Generador de UUID v4 en el cliente
function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0;
    var v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

// Al enviar el formulario
$( "#flight_delay_classification" ).submit(function( event ) {
  event.preventDefault();

  var $form = $( this ),
    url = $form.attr( "action" );

  // 1. Generar UUID en el navegador
  var uuid = generateUUID();

  // 2. Unirse a la sala PRIMERO y esperar confirmacion del servidor
  socket.emit('join', {uuid: uuid}, function() {
    // 3. Solo cuando el servidor confirma el join, mandar el POST con el UUID
    $( "#result" ).empty().append( "Processing..." );

    var formData = $( "#flight_delay_classification" ).serialize() + "&UUID=" + encodeURIComponent(uuid);
    $.post(url, formData);
  });
});

// Mostrar el resultado en la pagina segun la categoria predicha
function renderPage(response) {
  var displayMessage;

  if(response.Prediction == 0 || response.Prediction == '0') {
    displayMessage = "Early (15+ Minutes Early)";
  }
  else if(response.Prediction == 1 || response.Prediction == '1') {
    displayMessage = "Slightly Early (0-15 Minute Early)";
  }
  else if(response.Prediction == 2 || response.Prediction == '2') {
    displayMessage = "Slightly Late (0-30 Minute Delay)";
  }
  else if(response.Prediction == 3 || response.Prediction == '3') {
    displayMessage = "Very Late (30+ Minutes Late)";
  }

  $( "#result" ).empty().append( displayMessage );
}