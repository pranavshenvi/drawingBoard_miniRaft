const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");
const path = require("path");

const PORT = 8080;
const REQUEST_TIMEOUT_MS = 1000;
const replicaUrls = (process.env.REPLICA_URLS || "http://replica1:5001,http://replica2:5002,http://replica3:5003")
    .split(",")
    .map((url) => url.trim())
    .filter(Boolean);
let cachedLeaderUrl = null;

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

        // Keep client canvases in sync even during transient replica elections.
        broadcast(JSON.stringify(stroke));

        try {

            await postToLeader(stroke);

        } catch (err) {

            console.log("Replica error:", err.response?.data || err.message);

        }

    });

    ws.on("close", () => {

        console.log("Client disconnected");
        clients.delete(ws);

    });

});

async function discoverLeaderUrl() {
    for (const url of replicaUrls) {
        try {
            const response = await axios.get(`${url}/status`, { timeout: REQUEST_TIMEOUT_MS });
            if (response.data && response.data.isLeader) {
                cachedLeaderUrl = url;
                return url;
            }
        } catch {
            // Skip unavailable replica.
        }
    }

    return null;
}

async function postToLeader(stroke) {
    const candidates = [cachedLeaderUrl, await discoverLeaderUrl()].filter(Boolean);

    for (const leaderUrl of candidates) {
        try {
            console.log("Sending stroke to leader", leaderUrl);
            const response = await axios.post(`${leaderUrl}/stroke`, stroke, {
                timeout: REQUEST_TIMEOUT_MS
            });
            cachedLeaderUrl = leaderUrl;
            return response;
        } catch (err) {
            const leaderHint = err.response?.data?.leaderUrl;
            if (leaderHint && leaderHint !== leaderUrl) {
                cachedLeaderUrl = leaderHint;
                try {
                    return await axios.post(`${leaderHint}/stroke`, stroke, {
                        timeout: REQUEST_TIMEOUT_MS
                    });
                } catch {
                    // Fall through and try rediscovery.
                }
            }

            cachedLeaderUrl = null;
        }
    }

    const discovered = await discoverLeaderUrl();
    if (!discovered) {
        throw new Error("No leader available");
    }

    return axios.post(`${discovered}/stroke`, stroke, {
        timeout: REQUEST_TIMEOUT_MS
    });
}

discoverLeaderUrl().then((leaderUrl) => {
    if (leaderUrl) {
        console.log("Initial leader discovered:", leaderUrl);
    } else {
        console.log("Leader not available yet");
    }
});

function broadcast(message) {

    console.log("Broadcasting to", clients.size, "clients");

    clients.forEach(client => {

        if (client.readyState === WebSocket.OPEN) {

            client.send(message);

        }

    });

}