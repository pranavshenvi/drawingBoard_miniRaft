const express = require("express");
const axios = require("axios");
const { appendStroke } = require("./logManager");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5001;
const NODE_ID = parseInt(process.env.NODE_ID || "1");

/* cluster */
const peers = {
    1: "http://replica1:5001",
    2: "http://replica2:5002",
    3: "http://replica3:5003"
};

let currentLeaderId = null;
let isLeader = false;

/* heartbeat tracking */
let lastHeartbeat = Date.now();
const HEARTBEAT_TIMEOUT = 3000;


// ============================
// 🟢 HEALTH
// ============================
app.get("/ping", (req, res) => res.send("ok"));


// ============================
// 👑 LEADER INFO
// ============================
app.get("/leader", (req, res) => {
    res.json({
        is_leader: isLeader,
        leader_id: currentLeaderId
    });
});


// ============================
// ❤️ HEARTBEAT RECEIVER
// ============================
app.post("/heartbeat", (req, res) => {
    const { leader_id } = req.body;

    currentLeaderId = leader_id;
    isLeader = NODE_ID === leader_id;

    lastHeartbeat = Date.now();

    res.json({ status: "ok" });
});


// ============================
// 🔍 GET ALIVE NODES
// ============================
async function getAlivePeers() {
    const requests = Object.entries(peers).map(async ([id, url]) => {
        if (parseInt(id) === NODE_ID) return { id: NODE_ID, alive: true };

        try {
            await axios.get(`${url}/ping`, { timeout: 1000 });
            return { id: parseInt(id), alive: true };
        } catch {
            return { id: parseInt(id), alive: false };
        }
    });

    const results = await Promise.all(requests);
    return results.filter(r => r.alive);
}


// ============================
// 🗳️ ELECTION (highest ID wins)
// ============================
async function startElection() {
    console.log(`Node ${NODE_ID} starting election`);

    const alivePeers = await getAlivePeers();

    const highestId = Math.max(...alivePeers.map(p => p.id));

    currentLeaderId = highestId;
    isLeader = NODE_ID === highestId;

    console.log(`New leader elected: Node ${currentLeaderId}`);
}


// ============================
// ⏱️ ELECTION TIMER
// ============================
setInterval(async () => {
    if (Date.now() - lastHeartbeat > HEARTBEAT_TIMEOUT) {
        await startElection();
    }
}, 2000);


// ============================
// 💓 LEADER HEARTBEAT SENDER
// ============================
setInterval(async () => {
    if (!isLeader) return;

    for (const [id, url] of Object.entries(peers)) {
        if (parseInt(id) === NODE_ID) continue;

        try {
            await axios.post(`${url}/heartbeat`, {
                leader_id: NODE_ID
            }, { timeout: 1000 });
        } catch {}
    }
}, 1000);


// ============================
// 📤 CLIENT WRITE
// ============================
app.post("/stroke", async (req, res) => {

    if (!isLeader) {
        return res.status(307).json({ leader_id: currentLeaderId });
    }

    const stroke = req.body;

    const alivePeers = await getAlivePeers();
    const totalNodes = Object.keys(peers).length;
    const majority = Math.floor(totalNodes / 2) + 1;

    // 🔥 DEGRADED MODE
    if (alivePeers.length < majority) {
        console.log("⚠️ Degraded mode");

        appendStroke(stroke);

        // ✅ ADD HERE (Python storage)
        try {
            await axios.post("http://python-service:6000/append", stroke);
        } catch (e) {
            console.log("Python storage failed (degraded mode)");
        }

        return res.json({
            status: "committed (degraded)",
            stroke
        });
    }

    // NORMAL REPLICATION
    let successCount = 1;

    await Promise.all(alivePeers.map(async (peer) => {
        if (peer.id === NODE_ID) return;

        try {
            await axios.post(`${peers[peer.id]}/appendEntries`, stroke);
            successCount++;
        } catch {}
    }));

    if (successCount >= majority) {
        appendStroke(stroke);

        // ✅ ADD HERE (Python storage)
        try {
            await axios.post("http://python-service:6000/append", stroke);
        } catch (e) {
            console.log("Python storage failed");
        }

        return res.json({
            status: "committed",
            stroke
        });
    } else {
        return res.status(500).json({
            error: "Failed to reach majority"
        });
    }
});


// ============================
// 📥 FOLLOWER REPLICATION
// ============================
app.post("/appendEntries", (req, res) => {
    const stroke = req.body;

    appendStroke(stroke);

    res.json({ status: "ack" });
});


// ============================
// 🚀 START
// ============================
app.listen(PORT, () => {
    console.log(`Replica ${NODE_ID} running on port ${PORT}`);
});

