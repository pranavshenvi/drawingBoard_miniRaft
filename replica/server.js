const express = require("express");
const axios = require("axios");
const { appendStroke } = require("./logManager");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5001;
const NODE_ID = String(process.env.NODE_ID || "1");
const REQUEST_TIMEOUT_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 900;
const CLIENT_COMMIT_TIMEOUT_MS = 3500;
const ELECTION_TIMEOUT_MIN_MS = 1800;
const ELECTION_TIMEOUT_MAX_MS = 3200;
const ELECTION_NODE_SKEW_MS = 250;

const replicaNodes = parseReplicaNodes(
    process.env.REPLICA_NODES || "1=http://replica1:5001,2=http://replica2:5002,3=http://replica3:5003"
);
const allNodeIds = Object.keys(replicaNodes)
    .map((id) => Number(id))
    .sort((a, b) => a - b)
    .map((id) => String(id));
const peers = allNodeIds
    .filter((id) => id !== NODE_ID)
    .map((id) => ({ id, url: replicaNodes[id] }));

let currentTerm = 0;
let votedFor = null;
let log = [];

let commitIndex = -1;
let lastApplied = -1;

let role = "follower";
let leaderId = null;

let electionTimer = null;
let heartbeatTimer = null;
let electionInProgress = false;

let nextIndex = {};
let matchIndex = {};

const pendingCommits = new Map();

function parseReplicaNodes(rawConfig) {
    return rawConfig
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean)
        .reduce((acc, entry) => {
            const [id, url] = entry.split("=").map((token) => token.trim());
            if (id && url) {
                acc[String(id)] = url;
            }
            return acc;
        }, {});
}

function majorityCount() {
    return Math.floor(allNodeIds.length / 2) + 1;
}

function randomElectionTimeout() {
    const randomPart = Math.floor(
        Math.random() * (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS + 1)
    ) + ELECTION_TIMEOUT_MIN_MS;
    const nodeSkew = Number(NODE_ID) * ELECTION_NODE_SKEW_MS;

    return randomPart + nodeSkew;
}

function clearTimer(timerRef) {
    if (timerRef) {
        clearTimeout(timerRef);
    }
}

function getLastLogIndex() {
    return log.length - 1;
}

function getLastLogTerm() {
    return log.length ? log[log.length - 1].term : 0;
}

function isCandidateLogUpToDate(candidateLastLogIndex, candidateLastLogTerm) {
    const localLastTerm = getLastLogTerm();
    const localLastIndex = getLastLogIndex();

    if (candidateLastLogTerm !== localLastTerm) {
        return candidateLastLogTerm > localLastTerm;
    }

    return candidateLastLogIndex >= localLastIndex;
}

function resetElectionTimer() {
    clearTimer(electionTimer);
    electionTimer = setTimeout(() => {
        startElection().catch((err) => {
            console.log("Election error:", err.message);
        });
    }, randomElectionTimeout());
}

function rejectPendingCommits(reason) {
    pendingCommits.forEach((pending, index) => {
        pending.reject(new Error(reason));
        pendingCommits.delete(index);
    });
}

function stepDown(newTerm, discoveredLeaderId = null) {
    if (newTerm > currentTerm) {
        currentTerm = newTerm;
        votedFor = null;
    }

    if (role === "leader") {
        rejectPendingCommits("leader-stepped-down");
    }

    role = "follower";
    leaderId = discoveredLeaderId;
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
    resetElectionTimer();
}

function applyCommittedEntries() {
    while (lastApplied < commitIndex) {
        lastApplied += 1;
        const entry = log[lastApplied];
        appendStroke(entry.command);

        const pending = pendingCommits.get(lastApplied);
        if (pending) {
            pending.resolve(entry.command);
            pendingCommits.delete(lastApplied);
        }
    }
}

function updateCommitIndexFromMajority() {
    const replicatedIndices = [getLastLogIndex()];

    peers.forEach((peer) => {
        replicatedIndices.push(matchIndex[peer.id] ?? -1);
    });

    replicatedIndices.sort((a, b) => b - a);
    const quorumIndex = replicatedIndices[majorityCount() - 1];

    if (
        quorumIndex > commitIndex &&
        quorumIndex >= 0 &&
        log[quorumIndex] &&
        log[quorumIndex].term === currentTerm
    ) {
        commitIndex = quorumIndex;
        applyCommittedEntries();
    }
}

async function replicateToPeer(peer) {
    if (role !== "leader") {
        return;
    }

    const currentNextIndex = nextIndex[peer.id] ?? log.length;
    const prevLogIndex = currentNextIndex - 1;
    const prevLogTerm = prevLogIndex >= 0 ? log[prevLogIndex].term : 0;
    const entries = log.slice(currentNextIndex);

    try {
        const response = await axios.post(
            `${peer.url}/raft/appendEntries`,
            {
                term: currentTerm,
                leaderId: NODE_ID,
                prevLogIndex,
                prevLogTerm,
                entries,
                leaderCommit: commitIndex
            },
            { timeout: REQUEST_TIMEOUT_MS }
        );

        const data = response.data || {};

        if (data.term > currentTerm) {
            stepDown(data.term, null);
            return;
        }

        if (role !== "leader") {
            return;
        }

        if (data.success) {
            const replicatedThrough = prevLogIndex + entries.length;
            matchIndex[peer.id] = replicatedThrough;
            nextIndex[peer.id] = replicatedThrough + 1;
            updateCommitIndexFromMajority();
        } else {
            nextIndex[peer.id] = Math.max(0, currentNextIndex - 1);
        }
    } catch {
        // Peer unavailable; retry on next heartbeat/election cycle.
    }
}

async function replicateToAllPeers() {
    await Promise.all(peers.map((peer) => replicateToPeer(peer)));
}

function becomeLeader() {
    role = "leader";
    leaderId = NODE_ID;

    peers.forEach((peer) => {
        nextIndex[peer.id] = log.length;
        matchIndex[peer.id] = -1;
    });

    clearTimer(electionTimer);

    clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
        replicateToAllPeers().catch((err) => {
            console.log("Heartbeat replication error:", err.message);
        });
    }, HEARTBEAT_INTERVAL_MS);

    replicateToAllPeers().catch((err) => {
        console.log("Initial replication error:", err.message);
    });

    console.log(`Replica ${NODE_ID} became leader for term ${currentTerm}`);
}

async function startElection() {
    if (role === "leader" || electionInProgress) {
        return;
    }

    electionInProgress = true;
    role = "candidate";
    currentTerm += 1;
    votedFor = NODE_ID;
    leaderId = null;

    const electionTerm = currentTerm;
    let votes = 1;

    resetElectionTimer();

    const request = {
        term: electionTerm,
        candidateId: NODE_ID,
        lastLogIndex: getLastLogIndex(),
        lastLogTerm: getLastLogTerm()
    };

    try {
        await Promise.all(
            peers.map(async (peer) => {
                try {
                    const response = await axios.post(`${peer.url}/raft/requestVote`, request, {
                        timeout: REQUEST_TIMEOUT_MS
                    });

                    const data = response.data || {};
                    if (data.term > currentTerm) {
                        stepDown(data.term, null);
                        return;
                    }

                    if (role !== "candidate" || currentTerm !== electionTerm) {
                        return;
                    }

                    if (data.voteGranted) {
                        votes += 1;
                        if (
                            role === "candidate" &&
                            currentTerm === electionTerm &&
                            votes >= majorityCount()
                        ) {
                            becomeLeader();
                        }
                    }
                } catch {
                    // No vote from unreachable peer.
                }
            })
        );

    } finally {
        electionInProgress = false;
    }
}

function waitForCommit(index) {
    return new Promise((resolve, reject) => {
        if (index <= commitIndex) {
            resolve(log[index] ? log[index].command : null);
            return;
        }

        const timeout = setTimeout(() => {
            pendingCommits.delete(index);
            reject(new Error("commit-timeout"));
        }, CLIENT_COMMIT_TIMEOUT_MS);

        pendingCommits.set(index, {
            resolve: (value) => {
                clearTimeout(timeout);
                resolve(value);
            },
            reject: (error) => {
                clearTimeout(timeout);
                reject(error);
            }
        });
    });
}

app.post("/stroke", async (req, res) => {
    if (role !== "leader") {
        return res.status(403).json({
            status: "not-leader",
            leaderId,
            leaderUrl: leaderId ? replicaNodes[leaderId] : null,
            term: currentTerm
        });
    }

    const strokeCommand = {
        ...req.body,
        strokeId: req.body.strokeId || `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    };

    const entry = {
        term: currentTerm,
        command: strokeCommand
    };

    log.push(entry);
    const entryIndex = log.length - 1;
    const commitPromise = waitForCommit(entryIndex);

    try {
        await replicateToAllPeers();
        const committedStroke = await commitPromise;
        const ackCount = 1 + peers.filter((peer) => (matchIndex[peer.id] ?? -1) >= entryIndex).length;

        return res.json({
            status: "committed",
            leaderId: NODE_ID,
            term: currentTerm,
            acknowledgements: ackCount,
            required: majorityCount(),
            stroke: committedStroke
        });
    } catch {
        if (role !== "leader") {
            return res.status(503).json({
                status: "leader-changed",
                leaderId,
                leaderUrl: leaderId ? replicaNodes[leaderId] : null,
                term: currentTerm
            });
        }

        return res.status(503).json({
            status: "insufficient-acks",
            required: majorityCount(),
            received: 1 + peers.filter((peer) => (matchIndex[peer.id] ?? -1) >= entryIndex).length,
            term: currentTerm
        });
    }
});

app.post("/raft/requestVote", (req, res) => {
    const { term, candidateId, lastLogIndex, lastLogTerm } = req.body || {};

    if (term == null || !candidateId) {
        return res.status(400).json({ status: "invalid-request" });
    }

    if (term < currentTerm) {
        return res.json({ term: currentTerm, voteGranted: false });
    }

    if (term > currentTerm) {
        stepDown(term, null);
    }

    const canVote = votedFor === null || votedFor === candidateId;
    const upToDate = isCandidateLogUpToDate(
        Number(lastLogIndex ?? -1),
        Number(lastLogTerm ?? 0)
    );

    if (canVote && upToDate) {
        votedFor = candidateId;
        resetElectionTimer();
        return res.json({ term: currentTerm, voteGranted: true });
    }

    return res.json({ term: currentTerm, voteGranted: false });
});

app.post("/raft/appendEntries", (req, res) => {
    const {
        term,
        leaderId: incomingLeaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit
    } = req.body || {};

    if (term == null || !incomingLeaderId) {
        return res.status(400).json({ status: "invalid-request" });
    }

    if (term < currentTerm) {
        // Convergence fallback: if this node has no known leader, adopt the sender
        // to avoid split-vote stalls in small demo clusters when one node is down.
        if (role === "leader" || leaderId) {
            return res.json({ term: currentTerm, success: false });
        }

        role = "follower";
        leaderId = String(incomingLeaderId);
        resetElectionTimer();
    } else if (term > currentTerm || role !== "follower") {
        stepDown(term, String(incomingLeaderId));
    }

    leaderId = String(incomingLeaderId);
    resetElectionTimer();

    const prevIndex = Number(prevLogIndex);
    const prevTerm = Number(prevLogTerm);
    const incomingEntries = Array.isArray(entries) ? entries : [];

    if (prevIndex >= 0) {
        if (!log[prevIndex] || log[prevIndex].term !== prevTerm) {
            return res.json({ term: currentTerm, success: false });
        }
    }

    let insertAt = prevIndex + 1;
    for (let i = 0; i < incomingEntries.length; i += 1) {
        const existing = log[insertAt];
        const incoming = incomingEntries[i];

        if (existing && existing.term !== incoming.term) {
            log = log.slice(0, insertAt);
            if (commitIndex >= log.length) {
                commitIndex = log.length - 1;
            }
            if (lastApplied > commitIndex) {
                lastApplied = commitIndex;
            }
        }

        if (!log[insertAt]) {
            log.push(incoming);
        }

        insertAt += 1;
    }

    const leaderCommitIndex = Number(leaderCommit ?? -1);
    if (leaderCommitIndex > commitIndex) {
        commitIndex = Math.min(leaderCommitIndex, getLastLogIndex());
        applyCommittedEntries();
    }

    return res.json({ term: currentTerm, success: true });
});

app.post("/appendEntries", (req, res) => {
    // Backward compatibility endpoint for older callers.
    const stroke = req.body;
    log.push({ term: currentTerm, command: stroke });
    commitIndex = getLastLogIndex();
    applyCommittedEntries();

    console.log("Backward-compatible appendEntries received");
    res.json({ status: "ack" });
});

app.get("/health", (_req, res) => {
    res.json({
        status: "ok",
        nodeId: NODE_ID,
        term: currentTerm,
        role
    });
});

app.get("/status", (_req, res) => {
    res.json({
        nodeId: NODE_ID,
        isLeader: role === "leader",
        role,
        term: currentTerm,
        leaderId,
        leaderUrl: leaderId ? replicaNodes[leaderId] : null,
        commitIndex,
        lastApplied,
        logLength: log.length
    });
});

app.get("/leader", (_req, res) => {
    res.json({
        leaderId,
        leaderUrl: leaderId ? replicaNodes[leaderId] : null,
        term: currentTerm
    });
});

app.listen(PORT, () => {
    console.log(`Replica ${NODE_ID} running on port ${PORT}`);
    resetElectionTimer();
});

process.on("SIGTERM", () => {
    clearTimer(electionTimer);
    clearInterval(heartbeatTimer);
    rejectPendingCommits("shutdown");
    process.exit(0);
});

process.on("SIGINT", () => {
    clearTimer(electionTimer);
    clearInterval(heartbeatTimer);
    rejectPendingCommits("shutdown");
    process.exit(0);
});
