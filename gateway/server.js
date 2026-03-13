const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");
const path = require("path");

const PORT = 8080;
const LEADER_URL = "http://replica1:5001/stroke";

const app = express();

/* serve frontend */
// console.log("Serving frontend from:", path.join(__dirname, "../fontend"));
// app.use(express.static(path.join(__dirname, "../fontend")));
console.log("Serving frontend from:", path.join(__dirname, "fontend"));
app.use(express.static(path.join(__dirname, "fontend")));

/* start server */
const server = app.listen(PORT, () => {
    console.log("Gateway started on port", PORT);
});

/* websocket server */
const wss = new WebSocket.Server({ server });

let clients = new Set();

wss.on("connection", (ws) => {

    console.log("Client connected");
    clients.add(ws);

    ws.on("message", async (message) => {

        console.log("Received stroke from client");

        const stroke = JSON.parse(message.toString());

        try {

            console.log("Sending stroke to leader");

            const response = await axios.post(LEADER_URL, stroke);

            broadcast(JSON.stringify(response.data.stroke));

        } catch (err) {

            console.log("Replica error:", err.message);

        }

    });

    ws.on("close", () => {

        console.log("Client disconnected");
        clients.delete(ws);

    });

});

function broadcast(message) {

    console.log("Broadcasting to", clients.size, "clients");

    clients.forEach(client => {

        if (client.readyState === WebSocket.OPEN) {

            client.send(message);

        }

    });

}