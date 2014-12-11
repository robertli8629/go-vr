(function() {
    var data = document.getElementById("messageData");
    var conn = new WebSocket("ws://{{.Host}}/");
    conn.onclose = function(evt) {
        data.textContent += "Connection closed\n";
    }
    conn.onmessage = function(evt) {
        console.log('Message received');
        data.textContent += evt.data + "\n";
    }
})();

