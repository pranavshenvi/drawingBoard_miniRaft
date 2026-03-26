const ELECTION_TIMEOUT_MIN = 1500;  // ms - can be shorter now with fast vote timeout
const ELECTION_TIMEOUT_MAX = 4000;  // ms - still wide range for spread
const HEARTBEAT_INTERVAL = 100;     // ms - fast heartbeats to maintain leadership

function randomElectionTimeout() {
    return Math.floor(
        Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
    ) + ELECTION_TIMEOUT_MIN;
}

// Longer timeout after failed election to let others try first
function randomBackoffTimeout() {
    return Math.floor(Math.random() * 5000) + 3000;  // 3-8 seconds
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
