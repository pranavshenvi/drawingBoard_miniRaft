const express = require("express");
const axios = require("axios");
const { appendStroke } = require("./logManager");

const app = express();
app.use(express.json());

// ============================
// ENV
// ============================
const PORT = parseInt(process.env.PORT || "5001");
const NODE_ID = parseInt(process.env.NODE_ID || "1");

// ============================
// PEERS
// ============================
const peers = {
    1: "http://replica1:5001",
    2: "http://replica2:5002",
    3: "http://replica3:5003"
};

// ============================
// STATE
// ============================
let currentLeaderId = null;
let isLeader = false;

let lastHeartbeat = 0;
const HEARTBEAT_TIMEOUT = 5000;

// ============================
// LOG
// ============================
const log = [];
const MAX_LOG_SIZE = 1000;

function appendStrokeLocal(stroke) {
    log.push(stroke);

    if (log.length > MAX_LOG_SIZE) {
        log.shift();
    }

    appendStroke(stroke);
}

// ============================
// HEALTH
// ============================
app.get("/ping", (req, res) => res.send("ok"));

// ============================
// LEADER INFO
// ============================
app.get("/leader", (req, res) => {
    res.json({
        is_leader: isLeader,
        leader_id: currentLeaderId
    });
});

// ============================
// DISCOVER LEADER ON START
// ============================
async function discoverLeaderOnStartup() {
    console.log(`🔍 Node ${NODE_ID} discovering leader...`);

    for (const [id, url] of Object.entries(peers)) {
        if (parseInt(id) === NODE_ID) continue;

        try {
            const res = await axios.get(`${url}/leader`, { timeout: 1000 });

            if (res.data.leader_id !== null) {
                currentLeaderId = res.data.leader_id;
                isLeader = NODE_ID === currentLeaderId;

                console.log(`✅ Found leader: Node ${currentLeaderId}`);

                if (!isLeader) {
                    await catchUpWithLeader();
                }
                return;
            }
        } catch {}
    }

    console.log("⚠️ No leader found at startup");
}

// ============================
// HEARTBEAT RECEIVER
// ============================
let lastSyncTime = 0;
const SYNC_INTERVAL = 5000;

app.post("/heartbeat", async (req, res) => {
    const { leader_id } = req.body;

    currentLeaderId = leader_id;
    isLeader = NODE_ID === leader_id;
    lastHeartbeat = Date.now();

    if (!isLeader && Date.now() - lastSyncTime > SYNC_INTERVAL) {
        lastSyncTime = Date.now();
        await catchUpWithLeader();
    }

    res.json({ status: "ok" });
});

// ============================
// GET ALIVE PEERS
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
// LEADER ELECTION (FINAL FIX)
// ============================
async function startElection() {
    if (isLeader) return;

    const alivePeers = await getAlivePeers();

    // 🔥 CASE 1: ONLY NODE ALIVE → SELF LEADER
    if (alivePeers.length === 1 && alivePeers[0].id === NODE_ID) {
        currentLeaderId = NODE_ID;
        isLeader = true;
        lastHeartbeat = Date.now();

        console.log(`👑 Node ${NODE_ID} became leader (alone)`);
        return;
    }

    // 🔥 CASE 2: NORMAL ELECTION
    const leaderAlive = Date.now() - lastHeartbeat < HEARTBEAT_TIMEOUT;
    if (leaderAlive) return;

    console.log(`⚡ Node ${NODE_ID} starting election`);

    if (alivePeers.length === 0) return;

    const highestId = Math.max(...alivePeers.map(p => p.id));

    currentLeaderId = highestId;
    isLeader = NODE_ID === highestId;
    lastHeartbeat = Date.now();

    console.log(`👑 Leader elected: Node ${currentLeaderId}`);
}

// ============================
// ELECTION TIMER (FIXED)
// ============================
function startElectionTimer() {
    const timeout = 3000 + Math.random() * 3000;

    setTimeout(async () => {
        if (!isLeader) {
            await startElection();
        }
        startElectionTimer();
    }, timeout);
}

// ============================
// HEARTBEAT SENDER
// ============================
setInterval(async () => {
    if (!isLeader) return;

    for (const [id, url] of Object.entries(peers)) {
        if (parseInt(id) === NODE_ID) continue;

        try {
            await axios.post(
                `${url}/heartbeat`,
                { leader_id: NODE_ID },
                { timeout: 1500 }
            );
        } catch {}
    }
}, 1500);

// ============================
// LOG SYNC
// ============================
async function catchUpWithLeader() {
    if (!currentLeaderId || NODE_ID === currentLeaderId) return;

    try {
        const res = await axios.get(`${peers[currentLeaderId]}/allStrokes`, {
            timeout: 1500
        });

        const strokes = res.data.strokes;

        if (strokes.length !== log.length) {
            log.length = 0;

            strokes.forEach(s => appendStrokeLocal(s));

            console.log(`🔄 Node ${NODE_ID} synced with leader`);
        }

    } catch (err) {
        console.log(`❌ Sync failed: ${err.message}`);
    }
}

// ============================
// GET LOG
// ============================
app.get("/allStrokes", (req, res) => {
    res.json({ strokes: log });
});

// ============================
// CLIENT WRITE
// ============================
app.post("/stroke", async (req, res) => {
    if (!isLeader) {
        return res.status(307).json({ leader_id: currentLeaderId });
    }

    const stroke = req.body;

    const totalNodes = Object.keys(peers).length;
    const majority = Math.floor(totalNodes / 2) + 1;

    let successCount = 1;

    await Promise.all(Object.entries(peers).map(async ([id, url]) => {
        if (parseInt(id) === NODE_ID) return;

        try {
            await axios.post(`${url}/appendEntries`, stroke, {
                timeout: 1500
            });
            successCount++;
        } catch {}
    }));

    if (successCount >= majority) {
        appendStrokeLocal(stroke);
        console.log(`✅ Committed (majority)`);
        return res.json({ status: "committed", stroke });
    }

    console.log("⚠️ Degraded commit");
    appendStrokeLocal(stroke);

    return res.json({ status: "committed (degraded)", stroke });
});

// ============================
// FOLLOWER APPEND
// ============================
app.post("/appendEntries", (req, res) => {
    appendStrokeLocal(req.body);
    res.json({ status: "ack" });
});

// ============================
// DEBUG
// ============================
setInterval(() => {
    console.log(
        `📊 Node ${NODE_ID} | Leader: ${currentLeaderId} | Log: ${log.length}`
    );
}, 8000);

// ============================
// START SERVER
// ============================
app.listen(PORT, async () => {
    console.log(`🚀 Replica ${NODE_ID} running on port ${PORT}`);

    await discoverLeaderOnStartup();
    startElectionTimer();
});