const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");
const path = require("path");

const PORT = 8080;

/* RAFT replicas */
const REPLICAS = {
    1: process.env.REPLICA1_URL || "http://172.28.0.2:5001",
    2: process.env.REPLICA2_URL || "http://172.28.0.3:5002",
    3: process.env.REPLICA3_URL || "http://172.28.0.4:5003"
};

let currentLeaderId = null;

const app = express();

/* serve frontend (FIXED TYPO) */
console.log("Serving frontend from:", path.join(__dirname, "fontend"));
app.use(express.static(path.join(__dirname, "fontend")));

/* API: cluster status */
app.get("/api/cluster-status", async (req, res) => {
    const status = await getClusterStatus();
    res.json(status);
});

/* start HTTP server */
const server = app.listen(PORT, () => {
    console.log("Gateway started on port", PORT);
});

/* WebSocket server */
const wss = new WebSocket.Server({ server });
let clients = new Set();

/* WebSocket connection */
wss.on("connection", (ws) => {
    console.log("Client connected");
    clients.add(ws);

    ws.on("message", async (message) => {
        try {
            const stroke = JSON.parse(message.toString());

            const response = await sendToLeader(stroke);

            if (response && response.data) {
                const strokeData = response.data.stroke || stroke;
                broadcast(JSON.stringify(strokeData));
            }

        } catch (err) {
            console.log("WS error:", err.message);
        }
    });

    ws.on("close", () => {
        console.log("Client disconnected");
        clients.delete(ws);
    });
});


// ============================
// 🔍 PARALLEL LEADER DISCOVERY
// ============================
async function discoverLeader() {
    const requests = Object.entries(REPLICAS).map(async ([nodeId, url]) => {
        try {
            const res = await axios.get(`${url}/leader`, { timeout: 4000 });
            return { nodeId: parseInt(nodeId), data: res.data, url };
        } catch {
            return null;
        }
    });

    const results = await Promise.all(requests);

    for (const result of results) {
        if (!result) continue;

        if (result.data.is_leader) {
            console.log(`Leader found: Node ${result.nodeId}`);
            return result.nodeId;
        }

        if (result.data.leader_id) {
            console.log(`Node ${result.nodeId} says leader is ${result.data.leader_id}`);
            return result.data.leader_id;
        }
    }

    console.log("No leader found (cluster may be electing)");
    return null;
}


// ============================
// 📤 SEND TO LEADER (ROBUST)
// ============================
async function sendToLeader(stroke, retries = 3) {
    for (let i = 0; i < retries; i++) {

        // Discover leader if unknown
        if (!currentLeaderId || !REPLICAS[currentLeaderId]) {
            currentLeaderId = await discoverLeader();

            // 🚨 fallback: try all nodes if no leader
            if (!currentLeaderId) {
                console.log(`Retry ${i + 1}: No leader → fallback mode`);

                for (const [nodeId, url] of Object.entries(REPLICAS)) {
                    try {
                        const res = await axios.post(`${url}/stroke`, stroke, { timeout: 5000 });
                        console.log(`Fallback success on Node ${nodeId}`);
                        return res;
                    } catch {}
                }

                await sleep(500);
                continue;
            }
        }

        const leaderUrl = REPLICAS[currentLeaderId];

        try {
            console.log(`Sending to leader Node ${currentLeaderId}`);

            const response = await axios.post(
                `${leaderUrl}/stroke`,
                stroke,
                { timeout: 7000 }
            );

            return response;

        } catch (err) {
            console.log(`Error with Node ${currentLeaderId}`);

            if (err.response) {
                console.log("Status:", err.response.status);
                console.log("Data:", err.response.data);

                // Leader redirect
                if (err.response.status === 307 && err.response.data.leader_id) {
                    currentLeaderId = err.response.data.leader_id;
                    continue;
                }
            }

            // reset leader and retry
            currentLeaderId = null;
            await sleep(500);
        }
    }

    throw new Error("Failed to send stroke after retries");
}


// ============================
// 📊 CLUSTER STATUS
// ============================
async function getClusterStatus() {
    const requests = Object.entries(REPLICAS).map(async ([nodeId, url]) => {
        try {
            const res = await axios.get(`${url}/leader`, { timeout: 3000 });

            return {
                node_id: parseInt(nodeId),
                url,
                ...res.data,
                healthy: true
            };
        } catch (err) {
            return {
                node_id: parseInt(nodeId),
                url,
                healthy: false,
                error: err.message
            };
        }
    });

    const replicas = await Promise.all(requests);

    const leader = replicas.find(r => r.is_leader)?.node_id || null;

    return {
        replicas,
        currentLeader: leader
    };
}


// ============================
// 📡 BROADCAST
// ============================
function broadcast(message) {
    console.log("Broadcasting to", clients.size, "clients");

    clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}


// ============================
// 🧰 UTILS
// ============================
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


// ============================
// 🔄 PERIODIC LEADER REFRESH
// ============================
setInterval(async () => {
    const newLeader = await discoverLeader();

    if (newLeader && newLeader !== currentLeaderId) {
        console.log(`Leader updated: ${currentLeaderId} → ${newLeader}`);
        currentLeaderId = newLeader;
    }
}, 5000);


// ============================
// 🚀 INITIAL DISCOVERY
// ============================
(async () => {
    console.log("Initial leader discovery...");
    await sleep(2000);
    currentLeaderId = await discoverLeader();
    console.log("Initial leader:", currentLeaderId);
})();