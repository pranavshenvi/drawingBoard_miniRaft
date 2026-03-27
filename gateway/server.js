const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");
const path = require("path");

const PORT = 8080;

// RAFT cluster nodes (node_id -> URL mapping)
const REPLICAS = {
    1: process.env.REPLICA1_URL || "http://172.28.0.2:5001",
    2: process.env.REPLICA2_URL || "http://172.28.0.3:5002",
    3: process.env.REPLICA3_URL || "http://172.28.0.4:5003"
};

let currentLeaderId = null;

const app = express();

/* serve frontend */
console.log("Serving frontend from:", path.join(__dirname, "fontend"));
app.use(express.static(path.join(__dirname, "fontend")));

/* API endpoint to get cluster status */
app.get("/api/cluster-status", async (req, res) => {
    const status = await getClusterStatus();
    res.json(status);
});

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
            const response = await sendToLeader(stroke);
            if (response && response.data && response.data.stroke) {
                broadcast(JSON.stringify(response.data.stroke));
            }
        } catch (err) {
            console.log("Error sending to leader:", err.message);
        }
    });

    ws.on("close", () => {
        console.log("Client disconnected");
        clients.delete(ws);
    });
});

/**
 * Discover the current RAFT leader
 */
async function discoverLeader() {
    for (const [nodeId, url] of Object.entries(REPLICAS)) {
        try {
            const response = await axios.get(`${url}/leader`, { timeout: 2000 });

            if (response.data.is_leader) {
                // This node is the leader
                console.log(`Discovered leader: Node ${nodeId} at ${url}`);
                return parseInt(nodeId);
            } else if (response.data.leader_id) {
                // This node knows who the leader is
                console.log(`Node ${nodeId} says leader is Node ${response.data.leader_id}`);
                return response.data.leader_id;
            }
        } catch (err) {
            console.log(`Could not reach Node ${nodeId} at ${url}: ${err.message}`);
        }
    }
    console.log("No leader found, cluster may be electing...");
    return null;
}

/**
 * Send stroke to the current leader, with automatic leader discovery
 */
async function sendToLeader(stroke, retries = 3) {
    for (let i = 0; i < retries; i++) {
        // Discover leader if not known
        if (!currentLeaderId || !REPLICAS[currentLeaderId]) {
            currentLeaderId = await discoverLeader();
            if (!currentLeaderId) {
                console.log(`Retry ${i + 1}/${retries}: No leader, waiting...`);
                await sleep(500);
                continue;
            }
        }

        const leaderUrl = REPLICAS[currentLeaderId];

        try {
            console.log(`Sending stroke to leader Node ${currentLeaderId} at ${leaderUrl}`);
            const response = await axios.post(`${leaderUrl}/stroke`, stroke, { timeout: 5000 });
            return response;
        } catch (err) {
            console.log(`Failed to send to Node ${currentLeaderId}: ${err.message}`);

            // If not leader response (307), rediscover
            if (err.response && err.response.status === 307) {
                const newLeaderId = err.response.data.leader_id;
                console.log(`Node ${currentLeaderId} says leader is Node ${newLeaderId}`);
                currentLeaderId = newLeaderId;
            } else {
                // Node might be down, clear and retry
                console.log("Leader may be down, rediscovering...");
                currentLeaderId = null;
                await sleep(500);
            }
        }
    }

    throw new Error("Could not send stroke to any leader after retries");
}

/**
 * Get cluster status from all replicas
 */
async function getClusterStatus() {
    const status = {
        replicas: [],
        currentLeader: null
    };

    for (const [nodeId, url] of Object.entries(REPLICAS)) {
        try {
            const response = await axios.get(`${url}/leader`, { timeout: 2000 });
            status.replicas.push({
                node_id: parseInt(nodeId),
                url: url,
                ...response.data,
                healthy: true
            });
            if (response.data.is_leader) {
                status.currentLeader = parseInt(nodeId);
            }
        } catch (err) {
            status.replicas.push({
                node_id: parseInt(nodeId),
                url: url,
                healthy: false,
                error: err.message
            });
        }
    }

    return status;
}

function broadcast(message) {
    console.log("Broadcasting to", clients.size, "clients");

    clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Periodically refresh leader discovery
setInterval(async () => {
    const newLeaderId = await discoverLeader();
    if (newLeaderId && newLeaderId !== currentLeaderId) {
        console.log(`Leader updated: Node ${currentLeaderId} -> Node ${newLeaderId}`);
        currentLeaderId = newLeaderId;
    }
}, 5000);

// Initial leader discovery
(async () => {
    console.log("Performing initial leader discovery...");
    await sleep(2000); // Wait for replicas to start
    currentLeaderId = await discoverLeader();
    console.log(`Initial leader: Node ${currentLeaderId}`);
})();
