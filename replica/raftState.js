const STATES = {
    FOLLOWER: 'follower',
    CANDIDATE: 'candidate',
    LEADER: 'leader'
};

class RaftState {
    constructor(nodeId, peers) {
        // Persistent state (in production, should survive restarts)
        this.currentTerm = 0;
        this.votedFor = null;

        // Volatile state
        this.state = STATES.FOLLOWER;
        this.nodeId = nodeId;
        this.peers = peers;  // Array of {id, url}
        this.leaderId = null;

        // Leader-specific state
        this.nextIndex = {};
        this.matchIndex = {};
    }

    becomeFollower(term, leaderId = null) {
        console.log(`[Node ${this.nodeId}] Becoming FOLLOWER (term ${term}, leader: ${leaderId})`);
        this.state = STATES.FOLLOWER;
        this.currentTerm = term;
        this.votedFor = null;
        this.leaderId = leaderId;
    }

    becomeCandidate() {
        this.state = STATES.CANDIDATE;
        this.currentTerm++;
        this.votedFor = null;  // Don't vote for self - let peers decide
        this.leaderId = null;
        console.log(`[Node ${this.nodeId}] Becoming CANDIDATE (term ${this.currentTerm})`);
    }

    becomeLeader() {
        console.log(`[Node ${this.nodeId}] Becoming LEADER (term ${this.currentTerm})`);
        this.state = STATES.LEADER;
        this.leaderId = this.nodeId;
    }

    isLeader() {
        return this.state === STATES.LEADER;
    }

    isFollower() {
        return this.state === STATES.FOLLOWER;
    }

    isCandidate() {
        return this.state === STATES.CANDIDATE;
    }
}

module.exports = { RaftState, STATES };
