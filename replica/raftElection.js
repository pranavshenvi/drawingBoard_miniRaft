const axios = require('axios');
const { STATES } = require('./raftState');

async function startElection(raft, logManager, onBecomeLeader, onBecomeFollower, onElectionFailed) {
    raft.becomeCandidate();

    let votesYes = 0;
    let votesNo = 0;
    let responsesReceived = 0;
    let electionEnded = false;

    const voteRequest = {
        term: raft.currentTerm,
        candidateId: raft.nodeId,
        lastLogIndex: logManager.getLastLogIndex(),
        lastLogTerm: logManager.getLastLogTerm()
    };

    console.log(`[Node ${raft.nodeId}] Starting election for term ${raft.currentTerm}`);

    // Request votes from all peers in parallel with SHORT timeout
    const votePromises = raft.peers.map(async (peer) => {
        try {
            const response = await axios.post(
                `${peer.url}/requestVote`,
                voteRequest,
                { timeout: 300 }  // Short timeout - don't wait for dead servers
            );

            if (electionEnded) return;

            responsesReceived++;

            if (response.data.term > raft.currentTerm) {
                console.log(`[Node ${raft.nodeId}] Discovered higher term ${response.data.term} from ${peer.id}`);
                electionEnded = true;
                raft.becomeFollower(response.data.term);
                if (onBecomeFollower) onBecomeFollower();
                return;
            }

            if (response.data.voteGranted) {
                console.log(`[Node ${raft.nodeId}] Received vote from ${peer.id}`);
                votesYes++;
            } else {
                console.log(`[Node ${raft.nodeId}] Vote rejected by ${peer.id}`);
                votesNo++;
            }

            // Check if we won: majority of responses are YES
            checkElectionResult();

        } catch (err) {
            // Dead server - don't count it, don't wait for it
            if (err.code !== 'ECONNABORTED' && err.code !== 'ECONNREFUSED') {
                console.log(`[Node ${raft.nodeId}] Vote request to ${peer.id} failed: ${err.message}`);
            }
        }
    });

    function checkElectionResult() {
        if (electionEnded) return;
        if (raft.state !== STATES.CANDIDATE) return;

        // Need majority of RESPONDING nodes (not total nodes)
        const totalResponses = votesYes + votesNo;
        if (totalResponses === 0) return;

        const majorityNeeded = Math.floor(totalResponses / 2) + 1;

        if (votesYes >= majorityNeeded) {
            electionEnded = true;
            console.log(`[Node ${raft.nodeId}] Won election with ${votesYes}/${totalResponses} votes!`);
            raft.becomeLeader();
            if (onBecomeLeader) onBecomeLeader();
        } else if (votesNo >= majorityNeeded) {
            // Majority rejected us - fail early
            electionEnded = true;
            console.log(`[Node ${raft.nodeId}] Lost election: ${votesNo} rejections`);
            if (onElectionFailed) onElectionFailed();
        }
    }

    // Wait for all vote requests to complete (or timeout)
    await Promise.all(votePromises);

    // Final check after all responses
    if (!electionEnded && raft.state === STATES.CANDIDATE) {
        checkElectionResult();

        if (!electionEnded) {
            console.log(`[Node ${raft.nodeId}] Election inconclusive: ${votesYes} yes, ${votesNo} no, ${raft.peers.length - responsesReceived} no response`);
            if (onElectionFailed) onElectionFailed();
        }
    }
}

module.exports = { startElection };
