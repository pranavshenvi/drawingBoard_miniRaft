let socket;

function connectWebSocket() {

    socket = new WebSocket(`ws://${window.location.host}`);

    socket.onopen = () => {
        console.log("Connected to gateway");
    };

    socket.onmessage = (event) => {

        const stroke = JSON.parse(event.data);

        drawRemoteStroke(stroke);
    };

    socket.onclose = () => {
        console.log("Disconnected from gateway");
    };

}

function sendStroke(stroke) {

    if (socket && socket.readyState === WebSocket.OPEN) {

        socket.send(JSON.stringify(stroke));

    }

}