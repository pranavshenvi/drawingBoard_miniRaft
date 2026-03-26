const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");
const path = require("path");

const PORT = 8080;

// All replicas for leader discovery
const REPLICAS = [
    "http://replica1:5001",
    "http://replica2:5002",
    "http://replica3:5003",
    "http://replica4:5004",
    "http://replica5:5005"
];

let currentLeaderUrl = null;
let failedLeaderUrl = null;

const app = express();

// Global error handlers to prevent crashes
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err.message);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection:', reason);
});

/* serve frontend */
console.log("Serving frontend from:", path.join(__dirname, "fontend"));
app.use(express.static(path.join(__dirname, "fontend")));

/* start server */
const server = app.listen(PORT, () => {
    console.log("Gateway started on port", PORT);
    discoverLeader().catch(err => console.log("Initial discovery failed:", err.message));
});

/* websocket server */
const wss = new WebSocket.Server({ server });

let clients = new Set();

wss.on("connection", (ws) => {
    console.log("Client connected");
    clients.add(ws);

    ws.on("message", async (message) => {
        console.log("Received stroke from client");

        let stroke;
        try {
            stroke = JSON.parse(message.toString());
        } catch (parseErr) {
            console.log("Invalid JSON from client");
            return;
        }

        try {
            const response = await sendToLeader(stroke);
            console.log("Leader response:", response.status, JSON.stringify(response.data));
            if (response && response.data && response.data.stroke) {
                console.log("Broadcasting stroke to clients");
                broadcast(JSON.stringify(response.data.stroke));
            } else {
                console.log("No stroke in response data:", response.data);
            }
        } catch (err) {
            console.log("Failed to process stroke:", err.message);
            // Only send error if websocket is still open
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ error: "Failed to save stroke", message: err.message }));
            }
        }
    });

    ws.on("close", () => {
        console.log("Client disconnected");
        clients.delete(ws);
    });

    ws.on("error", (err) => {
        console.log("WebSocket error:", err.message);
        clients.delete(ws);
    });
});

// ==================== LEADER MANAGEMENT ====================

async function discoverLeader(excludeUrl = null) {
    console.log("Discovering leader..." + (excludeUrl ? ` (excluding ${excludeUrl})` : ""));

    for (const replicaUrl of REPLICAS) {
        // Skip the replica that just failed (it's the dead leader)
        if (excludeUrl && replicaUrl === excludeUrl) {
            continue;
        }

        try {
            const response = await axios.get(`${replicaUrl}/leader`, { timeout: 1000 });
            if (response.data && response.data.leaderUrl) {
                // Skip if the discovered leader is the one we know is dead
                if (excludeUrl && response.data.leaderUrl === excludeUrl) {
                    console.log(`Skipping stale leader ${response.data.leaderUrl}`);
                    continue;
                }
                currentLeaderUrl = response.data.leaderUrl;
                console.log(`Discovered leader: ${currentLeaderUrl} (term ${response.data.term})`);
                return currentLeaderUrl;
            }
        } catch (err) {
            console.log(`Could not get leader from ${replicaUrl}: ${err.message}`);
        }
    }

    console.log("No leader found, will retry...");
    currentLeaderUrl = null;
    return null;
}

async function sendToLeader(stroke, retries = 5) {
    let lastError = null;

    for (let attempt = 0; attempt < retries; attempt++) {
        // Ensure we have a leader URL
        if (!currentLeaderUrl) {
            // Wait longer on later attempts to allow election to complete
            if (attempt > 0) {
                const waitTime = 500 + (attempt * 200);  // 700ms, 900ms, 1100ms, 1300ms
                console.log(`[Attempt ${attempt + 1}] Waiting ${waitTime}ms for election...`);
                await sleep(waitTime);
            }
            await discoverLeader(failedLeaderUrl);
            if (!currentLeaderUrl) {
                lastError = new Error("No leader available");
                continue;
            }
        }

        try {
            console.log(`[Attempt ${attempt + 1}] Sending stroke to leader: ${currentLeaderUrl}`);
            const response = await axios.post(
                `${currentLeaderUrl}/stroke`,
                stroke,
                {
                    timeout: 3000,
                    maxRedirects: 0,
                    validateStatus: () => true  // Accept all status codes
                }
            );

            // Handle 307 redirect (not the leader)
            if (response.status === 307 && response.data && response.data.leaderUrl) {
                console.log(`Redirected to new leader: ${response.data.leaderUrl}`);
                currentLeaderUrl = response.data.leaderUrl;
                continue;
            }

            // Handle 503 (no leader available)
            if (response.status === 503) {
                console.log("Node says no leader available, waiting for election...");
                currentLeaderUrl = null;
                await sleep(500);
                continue;
            }

            // Handle 500 (replication failed)
            if (response.status === 500) {
                console.log("Leader failed to replicate, retrying...");
                lastError = new Error("Replication failed");
                continue;
            }

            // Success (2xx)
            if (response.status >= 200 && response.status < 300) {
                failedLeaderUrl = null;
                return response;
            }

            // Other error
            console.log(`Unexpected status ${response.status}:`, response.data);
            lastError = new Error(`Unexpected status: ${response.status}`);

        } catch (err) {
            console.log(`Connection error to ${currentLeaderUrl}: ${err.code || err.message}`);
            failedLeaderUrl = currentLeaderUrl;
            currentLeaderUrl = null;
            lastError = err;
        }
    }

    throw lastError || new Error("Failed to send stroke after " + retries + " retries");
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function broadcast(message) {
    console.log("Broadcasting to", clients.size, "clients");
    clients.forEach(client => {
        try {
            if (client.readyState === WebSocket.OPEN) {
                client.send(message);
            }
        } catch (err) {
            console.log("Broadcast error:", err.message);
        }
    });
}

// Periodic leader refresh (background)
setInterval(() => {
    if (!currentLeaderUrl) {
        discoverLeader().catch(err => console.log("Background discovery failed:", err.message));
    }
}, 2000);
