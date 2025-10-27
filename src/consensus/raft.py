# --- src/consensus/raft.py ---
import asyncio
import random
import time
import aiohttp
import logging
from enum import Enum
from typing import List, Dict, Any

# Import konfigurasi
try:
    from src.utils import config
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from utils import config

logger = logging.getLogger(__name__)


class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class RaftNode:
    """Inti state machine Raft (sederhana)."""

    def __init__(self, node_id: str, port: int) -> None:
        self.node_id = node_id
        self.port = port
        self.self_url = f"http://{self.node_id}:{self.port}"
        self.state = NodeState.FOLLOWER

        # Persistent state
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: List[Dict[str, Any]] = []  # {'term': int, 'command': dict}

        # Volatile state
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = self._get_new_election_timeout()
        self.current_leader_url: str | None = None

        # Leader-only volatile state (initialized when become leader)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Other nodes
        self.other_node_urls: List[str] = [
            u for u in config.RAFT_NODES if self.node_id not in u
        ]

        logger.info("[%s] initialized as %s", self.node_id, self.state.name)
        logger.debug("[%s] majority=%s other_nodes=%s", self.node_id, config.RAFT_MAJORITY, self.other_node_urls)

    def _get_new_election_timeout(self) -> float:
        return random.uniform(1.5, 3.0)

    def _reset_election_timer(self) -> None:
        self.last_heartbeat = time.time()
        self.election_timeout = self._get_new_election_timeout()

    async def run_timer(self) -> None:
        """Loop background untuk cek timeout dan (jika leader) kirim heartbeat."""
        while True:
            await asyncio.sleep(0.1)

            now = time.time()
            if self.state == NodeState.LEADER:
                if (now - self.last_heartbeat) > 0.5:
                    await self.send_heartbeats()
                    self.last_heartbeat = now
            else:
                if (now - self.last_heartbeat) > self.election_timeout:
                    logger.info("[%s] election timeout (state=%s)", self.node_id, self.state.name)
                    await self.start_election()

    async def start_election(self) -> None:
        """Mulai proses election sebagai kandidat."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # vote untuk diri sendiri
        self._reset_election_timer()
        self.current_leader_url = None

        logger.info("[%s] starting election term=%s", self.node_id, self.current_term)

        tasks = [self.send_vote_request(url) for url in self.other_node_urls]
        if tasks:
            await asyncio.gather(*tasks)

    async def send_vote_request(self, node_url: str) -> None:
        """Kirim RequestVote RPC ke satu node."""
        payload = {"term": self.current_term, "candidate_id": self.self_url}
        timeout = aiohttp.ClientTimeout(total=1.0)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{node_url}/rpc/request_vote", json=payload) as resp:
                    if resp.status != 200:
                        logger.debug("[%s] vote request to %s returned %s", self.node_id, node_url, resp.status)
                        return
                    data = await resp.json()
        except Exception as exc:
            logger.debug("[%s] vote request to %s failed: %s", self.node_id, node_url, exc)
            return

        if data.get("term", 0) > self.current_term:
            # Jika ada term lebih besar, turun ke follower
            self.current_term = data["term"]
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self.current_leader_url = None
            logger.info("[%s] stepped down to follower due to higher term %s", self.node_id, self.current_term)
            return

        if data.get("vote_granted") and data.get("term") == self.current_term:
            self.votes_received += 1
            logger.info("[%s] got vote from %s (total=%s)", self.node_id, node_url, self.votes_received)
            if self.state == NodeState.CANDIDATE and self.votes_received >= config.RAFT_MAJORITY:
                await self.become_leader()

    async def send_heartbeats(self) -> None:
        """Kirim AppendEntries kosong ke semua follower sebagai heartbeat."""
        tasks = [self.send_append_entries(url, []) for url in self.other_node_urls]
        if tasks:
            await asyncio.gather(*tasks)

    async def send_append_entries(self, node_url: str, entries: List[Dict[str, Any]]) -> bool:
        """Kirim AppendEntries RPC. Kembalikan True jika follower ack success."""
        payload = {"term": self.current_term, "leader_id": self.self_url, "entries": entries}
        timeout = aiohttp.ClientTimeout(total=0.5)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{node_url}/rpc/append_entries", json=payload) as resp:
                    if resp.status != 200:
                        logger.debug("[%s] append to %s returned %s", self.node_id, node_url, resp.status)
                        return False
                    data = await resp.json()
                    return bool(data.get("success", False))
        except Exception as exc:
            logger.debug("[%s] append to %s failed: %s", self.node_id, node_url, exc)
            return False

    async def become_leader(self) -> None:
        """Transisi ke leader dan kirim segera heartbeat."""
        if self.state != NodeState.CANDIDATE:
            return

        self.state = NodeState.LEADER
        self.current_leader_url = self.self_url
        # Inisialisasi indeks jika diperlukan
        for url in self.other_node_urls:
            self.next_index[url] = len(self.log) + 1
            self.match_index[url] = 0

        logger.info("[%s] became leader for term %s", self.node_id, self.current_term)
        await self.send_heartbeats()
        self.last_heartbeat = time.time()

    async def replicate_log_entry(self, command: Dict[str, Any]) -> bool:
        """(Leader) Tambah entry ke log dan replikasi ke mayoritas."""
        if self.state != NodeState.LEADER:
            return False

        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)

        tasks = [self.send_append_entries(url, [entry]) for url in self.other_node_urls]
        results = await asyncio.gather(*tasks)
        success_count = 1 + sum(1 for r in results if r)

        if success_count >= config.RAFT_MAJORITY:
            logger.info("[%s] log replicated to majority (%s)", self.node_id, success_count)
            # Di implementasi penuh, commit_index harus dinaikkan sesuai match_index
            return True

        logger.info("[%s] replication failed, confirmations=%s", self.node_id, success_count)
        return False

    # --- RPC handlers (dipanggil oleh server FastAPI) ---

    def handle_request_vote(self, term: int, candidate_id: str) -> Dict[str, Any]:
        """Balas RequestVote RPC."""
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        if term > self.current_term:
            logger.info("[%s] observed higher term %s, become follower", self.node_id, term)
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            self.current_leader_url = None

        vote_granted = False
        if self.voted_for is None or self.voted_for == candidate_id:
            vote_granted = True
            self.voted_for = candidate_id
            self._reset_election_timer()
            logger.info("[%s] granted vote to %s for term %s", self.node_id, candidate_id, term)

        return {"term": self.current_term, "vote_granted": vote_granted}

    def handle_append_entries(self, term: int, leader_id: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Balas AppendEntries RPC (termasuk heartbeat)."""
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self._reset_election_timer()
        self.current_leader_url = leader_id

        if self.state == NodeState.CANDIDATE:
            logger.info("[%s] received heartbeat from %s, revert to follower", self.node_id, leader_id)
            self.state = NodeState.FOLLOWER

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None

        if entries:
            logger.info("[%s] received %s log entries from %s", self.node_id, len(entries), leader_id)
            # Simplifikasi: langsung append tanpa cek konsistensi indeks
            self.log.extend(entries)

        return {"term": self.current_term, "success": True}