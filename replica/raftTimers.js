const ELECTION_TIMEOUT_MIN = 500;   // ms - per spec
const ELECTION_TIMEOUT_MAX = 800;   // ms - per spec
const HEARTBEAT_INTERVAL = 150;     // ms - per spec

function randomElectionTimeout() {
    return Math.floor(
        Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
    ) + ELECTION_TIMEOUT_MIN;
}

// Backoff after failed election
function randomBackoffTimeout() {
    return Math.floor(Math.random() * 500) + 800;  // 800-1300ms
}

class RaftTimers {
    constructor(callbacks) {
        this.callbacks = callbacks;  // { onElectionTimeout, onHeartbeat }
        this.electionTimer = null;
        this.heartbeatTimer = null;
    }

    resetElectionTimeout(useBackoff = false) {
        this.clearElectionTimeout();
        const timeout = useBackoff ? randomBackoffTimeout() : randomElectionTimeout();
        this.electionTimer = setTimeout(() => {
            if (this.callbacks.onElectionTimeout) {
                this.callbacks.onElectionTimeout();
            }
        }, timeout);
    }

    clearElectionTimeout() {
        if (this.electionTimer) {
            clearTimeout(this.electionTimer);
            this.electionTimer = null;
        }
    }

    startHeartbeats() {
        this.clearHeartbeats();
        // Send immediately, then at interval
        if (this.callbacks.onHeartbeat) {
            this.callbacks.onHeartbeat();
        }
        this.heartbeatTimer = setInterval(() => {
            if (this.callbacks.onHeartbeat) {
                this.callbacks.onHeartbeat();
            }
        }, HEARTBEAT_INTERVAL);
    }

    clearHeartbeats() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    stop() {
        this.clearElectionTimeout();
        this.clearHeartbeats();
    }
}

module.exports = { RaftTimers, HEARTBEAT_INTERVAL, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX };
