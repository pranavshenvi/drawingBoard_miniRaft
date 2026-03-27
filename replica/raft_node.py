import asyncio
import aiohttp
from aiohttp import web
import random
import sys
import json
import time

# Node configuration
NODE_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 1

NODES = {
    1: {"host": "172.28.0.2", "port": 5001},
    2: {"host": "172.28.0.3", "port": 5002},
    3: {"host": "172.28.0.4", "port": 5003}
}

# RAFT timing (from rules.txt)
ELECTION_TIMEOUT_MIN = 500   # ms
ELECTION_TIMEOUT_MAX = 800   # ms
HEARTBEAT_INTERVAL = 150     # ms

# Node states
FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of {term, stroke, committed}
        self.commit_index = -1
        self.leader_id = None

        # Election timer
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()

        # Leader state
        self.next_index = {}  # For each follower
        self.match_index = {}  # For each follower

        # Pending commits waiting for majority
        self.pending_commits = {}

        print(f"[Node {self.node_id}] Started as {self.state}, term={self.current_term}", flush=True)

    def _random_election_timeout(self):
        """Random timeout between 500-800ms"""
        return random.randint(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0

    def _reset_election_timer(self):
        """Reset election timeout"""
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()

    def _get_other_nodes(self):
        """Get list of other node IDs"""
        return [nid for nid in NODES.keys() if nid != self.node_id]

    def _get_node_url(self, node_id):
        """Get HTTP URL for a node"""
        node = NODES[node_id]
        return f"http://{node['host']}:{node['port']}"

    # ==================== REQUEST VOTE RPC ====================

    async def handle_request_vote(self, request):
        """Handle incoming vote request from candidate"""
        data = await request.json()
        candidate_term = data["term"]
        candidate_id = data["candidate_id"]
        last_log_index = data.get("last_log_index", -1)
        last_log_term = data.get("last_log_term", 0)

        vote_granted = False

        # Rule: Higher term always wins
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = FOLLOWER
            self.voted_for = None
            self.leader_id = None

        # Grant vote if:
        # 1. Candidate's term >= our term
        # 2. We haven't voted OR voted for this candidate
        # 3. Candidate's log is at least as up-to-date as ours
        if candidate_term >= self.current_term:
            if self.voted_for is None or self.voted_for == candidate_id:
                # Check log is up-to-date
                our_last_term = self.log[-1]["term"] if self.log else 0
                our_last_index = len(self.log) - 1

                log_ok = (last_log_term > our_last_term or
                         (last_log_term == our_last_term and last_log_index >= our_last_index))

                if log_ok:
                    vote_granted = True
                    self.voted_for = candidate_id
                    self._reset_election_timer()
                    print(f"[Node {self.node_id}] Voted for Node {candidate_id} in term {candidate_term}", flush=True)

        return web.json_response({
            "term": self.current_term,
            "vote_granted": vote_granted,
            "voter_id": self.node_id
        })

    async def request_vote_from(self, node_id):
        """Send vote request to another node"""
        url = f"{self._get_node_url(node_id)}/request-vote"
        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1]["term"] if self.log else 0

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "term": self.current_term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term
                }, timeout=aiohttp.ClientTimeout(total=0.5)) as resp:
                    return await resp.json()
        except Exception as e:
            print(f"[Node {self.node_id}] Failed to request vote from Node {node_id}: {e}", flush=True)
            return None

    # ==================== HEARTBEAT RPC ====================

    async def handle_heartbeat(self, request):
        """Handle heartbeat from leader"""
        data = await request.json()
        leader_term = data["term"]
        leader_id = data["leader_id"]

        # Rule: Higher term always wins
        if leader_term >= self.current_term:
            self.current_term = leader_term
            self.state = FOLLOWER
            self.leader_id = leader_id
            self.voted_for = None
            self._reset_election_timer()

            # Check if we need to catch up
            leader_commit = data.get("leader_commit", -1)
            if leader_commit > self.commit_index:
                print(f"[Node {self.node_id}] Behind leader, need sync. Our commit: {self.commit_index}, Leader: {leader_commit}", flush=True)

            return web.json_response({
                "term": self.current_term,
                "success": True,
                "node_id": self.node_id,
                "log_length": len(self.log)
            })
        else:
            # Our term is higher, reject
            return web.json_response({
                "term": self.current_term,
                "success": False,
                "node_id": self.node_id
            })

    async def send_heartbeat_to(self, node_id):
        """Send heartbeat to follower"""
        url = f"{self._get_node_url(node_id)}/heartbeat"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "leader_commit": self.commit_index
                }, timeout=aiohttp.ClientTimeout(total=0.3)) as resp:
                    result = await resp.json()

                    # Check if follower needs sync
                    if result.get("success") and result.get("log_length", 0) < len(self.log):
                        asyncio.create_task(self.sync_follower(node_id, result["log_length"]))

                    return result
        except Exception as e:
            return None

    # ==================== APPEND ENTRIES RPC ====================

    async def handle_append_entries(self, request):
        """Handle append entries from leader"""
        data = await request.json()
        leader_term = data["term"]
        leader_id = data["leader_id"]
        prev_log_index = data.get("prev_log_index", -1)
        prev_log_term = data.get("prev_log_term", 0)
        entries = data.get("entries", [])
        leader_commit = data.get("leader_commit", -1)

        # Rule: Higher term always wins
        if leader_term < self.current_term:
            return web.json_response({
                "term": self.current_term,
                "success": False,
                "node_id": self.node_id
            })

        # Update term and become follower
        self.current_term = leader_term
        self.state = FOLLOWER
        self.leader_id = leader_id
        self._reset_election_timer()

        # Check prev_log_index consistency
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                # Log is too short, need sync
                print(f"[Node {self.node_id}] Log too short. Have {len(self.log)}, need index {prev_log_index}", flush=True)
                return web.json_response({
                    "term": self.current_term,
                    "success": False,
                    "node_id": self.node_id,
                    "log_length": len(self.log),
                    "need_sync": True
                })

            if self.log[prev_log_index]["term"] != prev_log_term:
                # Term mismatch, truncate
                self.log = self.log[:prev_log_index]
                return web.json_response({
                    "term": self.current_term,
                    "success": False,
                    "node_id": self.node_id,
                    "log_length": len(self.log),
                    "need_sync": True
                })

        # Append new entries
        for entry in entries:
            self.log.append(entry)
            print(f"[Node {self.node_id}] Appended entry, log size: {len(self.log)}", flush=True)

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

        return web.json_response({
            "term": self.current_term,
            "success": True,
            "node_id": self.node_id,
            "match_index": len(self.log) - 1
        })

    async def send_append_entries(self, node_id, entries, prev_log_index=-1, prev_log_term=0):
        """Send append entries to follower"""
        url = f"{self._get_node_url(node_id)}/append-entries"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": self.commit_index
                }, timeout=aiohttp.ClientTimeout(total=1.0)) as resp:
                    return await resp.json()
        except Exception as e:
            print(f"[Node {self.node_id}] Failed to send append-entries to Node {node_id}: {e}", flush=True)
            return None

    # ==================== SYNC LOG RPC ====================

    async def handle_sync_log(self, request):
        """Handle sync request from follower (catch-up protocol)"""
        data = await request.json()
        from_index = data.get("from_index", 0)

        if self.state != LEADER:
            return web.json_response({
                "success": False,
                "message": "Not leader"
            })

        # Send all entries from the requested index
        entries = self.log[from_index:]
        print(f"[Node {self.node_id}] Syncing {len(entries)} entries to requester from index {from_index}", flush=True)

        return web.json_response({
            "success": True,
            "term": self.current_term,
            "entries": entries,
            "leader_commit": self.commit_index
        })

    async def sync_follower(self, node_id, follower_log_length):
        """Sync a follower that is behind"""
        url = f"{self._get_node_url(node_id)}/append-entries"

        # Get entries that follower is missing
        entries = self.log[follower_log_length:]
        prev_log_index = follower_log_length - 1
        prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 and prev_log_index < len(self.log) else 0

        if entries:
            print(f"[Node {self.node_id}] Syncing {len(entries)} entries to Node {node_id}", flush=True)
            await self.send_append_entries(node_id, entries, prev_log_index, prev_log_term)

    async def request_sync_from_leader(self):
        """Request sync from current leader (for restarted nodes)"""
        if self.leader_id is None:
            return

        url = f"{self._get_node_url(self.leader_id)}/sync-log"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "from_index": len(self.log)
                }, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                    result = await resp.json()

                    if result.get("success"):
                        entries = result.get("entries", [])
                        for entry in entries:
                            self.log.append(entry)
                        self.commit_index = result.get("leader_commit", -1)
                        print(f"[Node {self.node_id}] Synced {len(entries)} entries from leader, commit index: {self.commit_index}", flush=True)
        except Exception as e:
            print(f"[Node {self.node_id}] Failed to sync from leader: {e}", flush=True)

    # ==================== CLIENT STROKE HANDLING ====================

    async def handle_stroke(self, request):
        """Handle stroke from gateway - only leader accepts"""
        if self.state != LEADER:
            return web.json_response({
                "status": "not_leader",
                "leader_id": self.leader_id
            }, status=307)

        stroke = await request.json()

        # Step 1: Append to local log
        entry = {
            "term": self.current_term,
            "stroke": stroke,
            "index": len(self.log)
        }
        self.log.append(entry)
        entry_index = len(self.log) - 1

        print(f"[LEADER Node {self.node_id}] Received stroke, appended at index {entry_index}", flush=True)

        # Step 2: Replicate to followers
        acks = 1  # Self-ack
        prev_log_index = entry_index - 1
        prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0

        tasks = []
        for node_id in self._get_other_nodes():
            tasks.append(self.send_append_entries(node_id, [entry], prev_log_index, prev_log_term))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict) and result.get("success"):
                acks += 1

        # Step 3: Commit if majority (>=2 out of 3)
        if acks >= 2:
            self.commit_index = entry_index
            print(f"[LEADER Node {self.node_id}] Committed stroke at index {entry_index} with {acks} acks", flush=True)
            return web.json_response({
                "status": "committed",
                "stroke": stroke,
                "index": entry_index
            })
        else:
            print(f"[LEADER Node {self.node_id}] Failed to commit, only {acks} acks", flush=True)
            # Remove uncommitted entry
            self.log.pop()
            return web.json_response({
                "status": "failed",
                "message": f"Only {acks} acks, need majority"
            }, status=500)

    # ==================== LEADER INFO ====================

    async def handle_leader(self, request):
        """Return leader information"""
        return web.json_response({
            "leader_id": self.leader_id if self.state != LEADER else self.node_id,
            "is_leader": self.state == LEADER,
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "log_length": len(self.log),
            "commit_index": self.commit_index
        })

    async def handle_health(self, request):
        """Health check"""
        return web.json_response({"status": "healthy", "node_id": self.node_id})

    # ==================== ELECTION LOGIC ====================

    async def start_election(self):
        """Start leader election as candidate"""
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id  # Vote for self
        self._reset_election_timer()

        print(f"[Node {self.node_id}] Starting election for term {self.current_term}", flush=True)

        votes = 1  # Self-vote

        # Request votes from other nodes
        tasks = []
        for node_id in self._get_other_nodes():
            tasks.append(self.request_vote_from(node_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict):
                # Check if our term is stale
                if result.get("term", 0) > self.current_term:
                    self.current_term = result["term"]
                    self.state = FOLLOWER
                    self.voted_for = None
                    print(f"[Node {self.node_id}] Discovered higher term, becoming follower", flush=True)
                    return

                if result.get("vote_granted"):
                    votes += 1
                    print(f"[Node {self.node_id}] Got vote from Node {result.get('voter_id')}", flush=True)

        # Check if won election (majority = 2 out of 3)
        if votes >= 2 and self.state == CANDIDATE:
            self.state = LEADER
            self.leader_id = self.node_id
            print(f"[Node {self.node_id}] WON ELECTION with {votes} votes in term {self.current_term}", flush=True)

            # Initialize leader state
            for node_id in self._get_other_nodes():
                self.next_index[node_id] = len(self.log)
                self.match_index[node_id] = -1
        else:
            # Split vote, retry
            print(f"[Node {self.node_id}] Election failed with {votes} votes, retrying...", flush=True)
            self.state = FOLLOWER
            self.voted_for = None

    # ==================== MAIN LOOPS ====================

    async def election_timer_loop(self):
        """Check for election timeout"""
        while True:
            await asyncio.sleep(0.05)  # Check every 50ms

            if self.state == LEADER:
                continue

            elapsed = time.time() - self.last_heartbeat
            if elapsed >= self.election_timeout:
                print(f"[Node {self.node_id}] Election timeout ({elapsed:.3f}s), starting election", flush=True)
                await self.start_election()

    async def heartbeat_loop(self):
        """Send heartbeats as leader"""
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL / 1000.0)

            if self.state != LEADER:
                continue

            # Send heartbeats to all followers
            for node_id in self._get_other_nodes():
                asyncio.create_task(self.send_heartbeat_to(node_id))


# ==================== HTTP SERVER SETUP ====================

async def create_app(node):
    app = web.Application()
    app['node'] = node

    # RAFT RPCs
    app.router.add_post('/request-vote', node.handle_request_vote)
    app.router.add_post('/heartbeat', node.handle_heartbeat)
    app.router.add_post('/append-entries', node.handle_append_entries)
    app.router.add_post('/sync-log', node.handle_sync_log)

    # Client endpoints
    app.router.add_post('/stroke', node.handle_stroke)
    app.router.add_get('/leader', node.handle_leader)
    app.router.add_get('/health', node.handle_health)

    return app


async def main():
    node = RaftNode(NODE_ID)

    app = await create_app(node)

    runner = web.AppRunner(app)
    await runner.setup()

    port = NODES[NODE_ID]["port"]
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    print(f"[Node {NODE_ID}] HTTP server started on port {port}", flush=True)

    # Start background tasks
    asyncio.create_task(node.election_timer_loop())
    asyncio.create_task(node.heartbeat_loop())

    # Keep running
    while True:
        await asyncio.sleep(1)
        if node.state == LEADER:
            print(f"[Node {NODE_ID}] LEADER | term={node.current_term} | log={len(node.log)} | commit={node.commit_index}", flush=True)
        else:
            print(f"[Node {NODE_ID}] {node.state.upper()} | term={node.current_term} | leader={node.leader_id} | log={len(node.log)}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
