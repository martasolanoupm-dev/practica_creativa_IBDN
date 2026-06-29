// Conectar al servidor por WebSocket
var socket = io();

// Cuando llega una predicción para nuestra sala, mostrarla
socket.on('prediction', function(data) {
  renderPage(data);
});

// Al enviar el formulario
$( "#flight_delay_classification" ).submit(function( event ) {

  // Evitar el envío normal del formulario
  event.preventDefault();

  var $form = $( this ),
    url = $form.attr( "action" );

  // Enviar los datos del formulario por POST
  var posting = $.post(
    url,
    $( "#flight_delay_classification" ).serialize()
  );

  // Cuando el servidor responde con el UUID, unirse a la sala de ese UUID
  posting.done(function( data ) {
    var response = JSON.parse(data);
    if(response.status == "OK") {
      $( "#result" ).empty().append( "Processing..." );
      socket.emit('join', {uuid: response.id});
    }
  });
});

// Mostrar el resultado en la página según la categoría predicha
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