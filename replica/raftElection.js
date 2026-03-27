const axios = require('axios');
const { STATES } = require('./raftState');

async function startElection(raft, logManager, onBecomeLeader, onBecomeFollower, onElectionFailed) {
    raft.becomeCandidate();

    // Candidate votes for itself
    raft.votedFor = raft.nodeId;
    let votesReceived = 1;  // Self-vote
    let electionEnded = false;

    // Per spec: need majority (≥2) for 3 nodes
    const MAJORITY = 2;

    const voteRequest = {
        term: raft.currentTerm,
        candidateId: raft.nodeId,
        lastLogIndex: logManager.getLastLogIndex(),
        lastLogTerm: logManager.getLastLogTerm()
    };

    console.log(`[Node ${raft.nodeId}] Starting election for term ${raft.currentTerm}, need ${MAJORITY} votes`);

    // Request votes from all peers in parallel
    const votePromises = raft.peers.map(async (peer) => {
        try {
            const response = await axios.post(
                `${peer.url}/requestVote`,
                voteRequest,
                { timeout: 300 }  // Short timeout - don't wait for dead servers
            );

            if (electionEnded) return false;

            if (response.data.term > raft.currentTerm) {
                console.log(`[Node ${raft.nodeId}] Discovered higher term ${response.data.term} from ${peer.id}`);
                electionEnded = true;
                raft.becomeFollower(response.data.term);
                if (onBecomeFollower) onBecomeFollower();
                return false;
            }

            if (response.data.voteGranted) {
                console.log(`[Node ${raft.nodeId}] Received vote from ${peer.id}`);
                votesReceived++;

                // Check if we won (≥2 votes)
                if (!electionEnded && raft.state === STATES.CANDIDATE && votesReceived >= MAJORITY) {
                    electionEnded = true;
                    console.log(`[Node ${raft.nodeId}] Won election with ${votesReceived} votes!`);
                    raft.becomeLeader();
                    if (onBecomeLeader) onBecomeLeader();
                }
                return true;
            } else {
                console.log(`[Node ${raft.nodeId}] Vote rejected by ${peer.id}`);
                return false;
            }

        } catch (err) {
            // Dead server - don't count it, don't wait for it
            if (err.code !== 'ECONNABORTED' && err.code !== 'ECONNREFUSED') {
                console.log(`[Node ${raft.nodeId}] Vote request to ${peer.id} failed: ${err.message}`);
            }
            return false;
        }
    });

    // Wait for all vote requests to complete (or timeout)
    await Promise.all(votePromises);

    // If election hasn't ended (didn't win and didn't step down), it failed
    if (!electionEnded && raft.state === STATES.CANDIDATE) {
        console.log(`[Node ${raft.nodeId}] Election failed: only got ${votesReceived}/${MAJORITY} votes`);
        if (onElectionFailed) onElectionFailed();
    }
}

module.exports = { startElection };
