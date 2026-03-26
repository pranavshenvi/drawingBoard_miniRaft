let socket;

function connectWebSocket() {

    socket = new WebSocket(`ws://${window.location.host}`);

    socket.onopen = () => {
        console.log("Connected to gateway");
    };

    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);

        // Ignore error messages
        if (data.error) {
            console.log("Server error:", data.message);
            return;
        }

        // Only draw if it has stroke data
        if (data.points && data.points.length > 0) {
            drawRemoteStroke(data);
        }
    };

    socket.onclose = () => {
        console.log("Disconnected from gateway, reconnecting...");
        // Auto-reconnect after 1 second
        setTimeout(connectWebSocket, 1000);
    };

}

function sendStroke(stroke) {

    if (socket && socket.readyState === WebSocket.OPEN) {

        socket.send(JSON.stringify(stroke));

    }

}