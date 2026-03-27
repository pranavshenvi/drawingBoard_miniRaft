const express = require("express");
const axios = require("axios");
const { RaftState, STATES } = require("./raftState");
const { RaftTimers } = require("./raftTimers");
const { startElection } = require("./raftElection");
const logManager = require("./logManager");

// Global error handlers to prevent crashes
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err.message);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection:', reason);
});

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8001;
const NODE_ID = process.env.NODE_ID || "1";

// All nodes in the cluster with static IPs (matching reference)
const ALL_NODES = [
    { id: "1", url: "http://172.28.0.2:8001" },
    { id: "2", url: "http://172.28.0.3:8002" },
    { id: "3", url: "http://172.28.0.4:8003" }
];

// Peers (all nodes except self)
const PEERS = ALL_NODES.filter(n => n.id !== NODE_ID);
const SELF = ALL_NODES.find(n => n.id === NODE_ID);

// Initialize Raft state
const raft = new RaftState(NODE_ID, PEERS);

// Initialize timers with callbacks
const timers = new RaftTimers({
    onElectionTimeout: () => {
        if (raft.state !== STATES.LEADER) {
            console.log(`[Node ${NODE_ID}] Election timeout! Starting election (was ${raft.state}, term ${raft.currentTerm})`);
            startElection(
                raft,
                logManager,
                () => {
                    // On become leader
                    console.log(`[Node ${NODE_ID}] === BECAME LEADER === (term ${raft.currentTerm})`);
                    timers.clearElectionTimeout();
                    timers.startHeartbeats();
                },
                () => {
                    // On become follower
                    timers.clearHeartbeats();
                    timers.resetElectionTimeout();
                },
                () => {
                    // On election failed (split vote) - retry with longer backoff
                    console.log(`[Node ${NODE_ID}] Election failed, backing off`);
                    timers.resetElectionTimeout(true);  // Use backoff
                }
            ).catch(err => {
                console.error(`[Node ${NODE_ID}] Election error:`, err.message);
                timers.resetElectionTimeout();
            });
        }
    },
    onHeartbeat: () => sendHeartbeats()
});

// ==================== ENDPOINTS ====================

// POST /stroke - Client stroke (leader only)
app.post("/stroke", async (req, res) => {
    try {
        if (raft.state !== STATES.LEADER) {
            // Redirect to leader if known
            if (raft.leaderId) {
                const leader = ALL_NODES.find(n => n.id === raft.leaderId);
                return res.status(307).json({
                    error: "Not leader",
                    leaderId: raft.leaderId,
                    leaderUrl: leader?.url
                });
            }
            return res.status(503).json({ error: "No leader available" });
        }

        const stroke = req.body;
        const entry = logManager.appendEntry(raft.currentTerm, stroke);

        // Replicate to followers
        const replicationResults = await replicateEntry(entry);
        const successCount = 1 + replicationResults.filter(r => r).length;
        const majority = Math.floor(ALL_NODES.length / 2) + 1;

        if (successCount >= majority) {
            console.log(`[Leader ${NODE_ID}] Stroke committed with ${successCount}/${ALL_NODES.length} replicas`);
            res.json({ status: "committed", stroke: entry.stroke, index: entry.index });
        } else {
            console.log(`[Leader ${NODE_ID}] Failed to reach majority: ${successCount}/${majority}`);
            res.status(500).json({ error: "Failed to replicate to majority" });
        }
    } catch (err) {
        console.error(`[Node ${NODE_ID}] Error in /stroke:`, err.message);
        res.status(500).json({ error: "Internal error" });
    }
});

// POST /requestVote - Raft vote request RPC
app.post("/requestVote", (req, res) => {
    const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;

    // Rule 1: Reply false if term < currentTerm
    if (term < raft.currentTerm) {
        return res.json({ term: raft.currentTerm, voteGranted: false });
    }

    // If higher term, step down to follower
    if (term > raft.currentTerm) {
        console.log(`[Node ${NODE_ID}] Higher term ${term} from ${candidateId}, stepping down`);
        raft.becomeFollower(term);
        timers.clearHeartbeats();
        timers.resetElectionTimeout(true);
    }

    // Tiebreaker: if we're a candidate at the same term, vote for lower ID
    if (raft.state === STATES.CANDIDATE && term === raft.currentTerm) {
        if (candidateId < NODE_ID) {
            // Other candidate has lower ID - step down and vote for them
            console.log(`[Node ${NODE_ID}] Yielding to lower-ID candidate ${candidateId}`);
            raft.state = STATES.FOLLOWER;
            raft.votedFor = null;  // Clear to allow voting below
            timers.resetElectionTimeout(true);
        } else {
            // We have lower ID - reject their vote
            console.log(`[Node ${NODE_ID}] Rejecting vote from higher-ID candidate ${candidateId}`);
            return res.json({ term: raft.currentTerm, voteGranted: false });
        }
    }

    // Check if already voted for someone else in this term
    if (raft.votedFor !== null && raft.votedFor !== candidateId) {
        // Tiebreaker: switch vote to lower ID candidate
        if (candidateId < raft.votedFor) {
            console.log(`[Node ${NODE_ID}] Switching vote from ${raft.votedFor} to lower-ID ${candidateId}`);
            raft.votedFor = candidateId;
            timers.resetElectionTimeout(true);
            return res.json({ term: raft.currentTerm, voteGranted: true });
        }
        console.log(`[Node ${NODE_ID}] Rejecting vote: already voted for lower-ID ${raft.votedFor}`);
        return res.json({ term: raft.currentTerm, voteGranted: false });
    }

    // Check if candidate's log is at least as up-to-date
    const myLastTerm = logManager.getLastLogTerm();
    const myLastIndex = logManager.getLastLogIndex();

    const logOk = (lastLogTerm > myLastTerm) ||
        (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex);

    if (logOk) {
        raft.votedFor = candidateId;
        timers.resetElectionTimeout(true);
        console.log(`[Node ${NODE_ID}] Granting vote to ${candidateId} in term ${term}`);
        return res.json({ term: raft.currentTerm, voteGranted: true });
    }

    console.log(`[Node ${NODE_ID}] Rejecting vote: log not up-to-date`);
    res.json({ term: raft.currentTerm, voteGranted: false });
});

// POST /appendEntries - Heartbeat and log replication
app.post("/appendEntries", (req, res) => {
    const { term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;

    // Rule 1: Reply false if term < currentTerm
    if (term < raft.currentTerm) {
        console.log(`[Node ${NODE_ID}] Rejecting appendEntries: stale term ${term} < ${raft.currentTerm}`);
        return res.json({ term: raft.currentTerm, success: false });
    }

    // Valid leader heartbeat - reset election timeout
    timers.resetElectionTimeout();

    // Step down if we see equal or higher term from a leader
    if (term >= raft.currentTerm) {
        if (raft.state !== STATES.FOLLOWER) {
            console.log(`[Node ${NODE_ID}] Stepping down from ${raft.state} to FOLLOWER (leader is ${leaderId})`);
            raft.becomeFollower(term, leaderId);
            timers.clearHeartbeats();
        } else if (raft.currentTerm < term) {
            console.log(`[Node ${NODE_ID}] Updating term from ${raft.currentTerm} to ${term}`);
            raft.becomeFollower(term, leaderId);
        }
        raft.leaderId = leaderId;
    }

    // If this is just a heartbeat (no entries), return success
    if (!entries || entries.length === 0) {
        return res.json({ term: raft.currentTerm, success: true, matchIndex: logManager.getLastLogIndex() });
    }

    // Log replication (not just heartbeat)
    console.log(`[Node ${NODE_ID}] Replicating ${entries.length} entries from leader ${leaderId}`);

    // Log consistency check
    if (prevLogIndex >= 0) {
        const prevEntry = logManager.getEntry(prevLogIndex);
        if (!prevEntry || prevEntry.term !== prevLogTerm) {
            console.log(`[Node ${NODE_ID}] Log inconsistency at index ${prevLogIndex}, my log length: ${logManager.getLogLength()}`);
            // Return current log length so leader can sync
            return res.json({
                term: raft.currentTerm,
                success: false,
                logLength: logManager.getLogLength(),
                needsSync: true
            });
        }
    }

    // Append new entries
    for (const entry of entries) {
        const existing = logManager.getEntry(entry.index);
        if (existing && existing.term !== entry.term) {
            // Conflict - delete this and all following entries
            logManager.deleteEntriesFrom(entry.index);
        }
        if (!logManager.getEntry(entry.index)) {
            logManager.appendEntry(entry.term, entry.stroke);
        }
    }

    const matchIndex = logManager.getLastLogIndex();
    console.log(`[Node ${NODE_ID}] Replicated ${entries.length} entries, matchIndex: ${matchIndex}`);
    res.json({ term: raft.currentTerm, success: true, matchIndex });
});

// GET /leader - Leader discovery
app.get("/leader", (req, res) => {
    if (raft.leaderId) {
        const leader = ALL_NODES.find(n => n.id === raft.leaderId);
        res.json({
            leaderId: raft.leaderId,
            leaderUrl: leader?.url,
            term: raft.currentTerm
        });
    } else {
        res.status(503).json({ error: "No leader elected", term: raft.currentTerm });
    }
});

// GET /status - Debug endpoint
app.get("/status", (req, res) => {
    res.json({
        nodeId: NODE_ID,
        state: raft.state,
        term: raft.currentTerm,
        leaderId: raft.leaderId,
        votedFor: raft.votedFor,
        logLength: logManager.getLogLength()
    });
});

// POST /sync-log - Catch-up protocol for restarted nodes
app.post("/sync-log", (req, res) => {
    const { entries } = req.body;

    if (!entries || !Array.isArray(entries)) {
        return res.status(400).json({ error: "Invalid entries" });
    }

    console.log(`[Node ${NODE_ID}] Syncing ${entries.length} entries from leader`);

    for (const entry of entries) {
        if (!logManager.getEntry(entry.index)) {
            logManager.appendEntry(entry.term, entry.stroke);
        }
    }

    console.log(`[Node ${NODE_ID}] Sync complete, log length: ${logManager.getLogLength()}`);
    res.json({ success: true, logLength: logManager.getLogLength() });
});

// ==================== HELPER FUNCTIONS ====================

async function replicateEntry(entry) {
    const promises = PEERS.map(async (peer) => {
        try {
            const prevLogIndex = entry.index - 1;
            const prevEntry = logManager.getEntry(prevLogIndex);

            const response = await axios.post(`${peer.url}/appendEntries`, {
                term: raft.currentTerm,
                leaderId: NODE_ID,
                prevLogIndex: prevLogIndex,
                prevLogTerm: prevEntry?.term || 0,
                entries: [entry],
                leaderCommit: entry.index
            }, { timeout: 2000 });

            return response.data.success;
        } catch (err) {
            console.log(`[Leader ${NODE_ID}] Replication to ${peer.id} failed: ${err.message}`);
            return false;
        }
    });

    return Promise.all(promises);
}

function sendHeartbeats() {
    if (raft.state !== STATES.LEADER) return;

    PEERS.forEach(async (peer) => {
        try {
            const response = await axios.post(`${peer.url}/appendEntries`, {
                term: raft.currentTerm,
                leaderId: NODE_ID,
                prevLogIndex: logManager.getLastLogIndex(),
                prevLogTerm: logManager.getLastLogTerm(),
                entries: [],  // Empty = heartbeat
                leaderCommit: logManager.getLastLogIndex()
            }, { timeout: 1000 });

            // Check if follower needs sync (restarted node)
            if (response.data && response.data.needsSync) {
                console.log(`[Leader ${NODE_ID}] Peer ${peer.id} needs sync from index ${response.data.logLength}`);
                await syncFollower(peer, response.data.logLength);
            }
        } catch (err) {
            // Silently ignore heartbeat failures
        }
    });
}

async function syncFollower(peer, fromIndex) {
    const entries = logManager.getEntriesFrom(fromIndex);
    if (entries.length === 0) return;

    console.log(`[Leader ${NODE_ID}] Syncing ${entries.length} entries to ${peer.id}`);
    try {
        await axios.post(`${peer.url}/sync-log`, { entries }, { timeout: 5000 });
        console.log(`[Leader ${NODE_ID}] Sync to ${peer.id} complete`);
    } catch (err) {
        console.log(`[Leader ${NODE_ID}] Sync to ${peer.id} failed: ${err.message}`);
    }
}

// ==================== STARTUP ====================

app.listen(PORT, () => {
    console.log(`Replica ${NODE_ID} running on port ${PORT}`);
    console.log(`Peers: ${PEERS.map(p => p.id).join(", ")}`);
    // Small random startup delay to stagger initial elections
    const startupDelay = Math.floor(Math.random() * 300);
    console.log(`[Node ${NODE_ID}] Starting election timer in ${startupDelay}ms`);
    setTimeout(() => {
        timers.resetElectionTimeout();
    }, startupDelay);
});
