# src/nodes/lock_manager.py
import sys
import os
import asyncio
import logging
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import redis.asyncio as redis

# Tambahkan path project saat dijalankan langsung
try:
    from src.utils import config
    from src.consensus.raft import RaftNode, NodeState
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from utils import config
    from consensus.raft import RaftNode, NodeState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Models untuk RPC antar-node
class RequestVoteArgs(BaseModel):
    term: int
    candidate_id: str

class AppendEntriesArgs(BaseModel):
    term: int
    leader_id: str
    entries: list

# Model untuk API client
class LockRequest(BaseModel):
    lock_name: str
    client_id: str
    timeout_sec: int = 10  # lease time

app = FastAPI(title=f"Distributed Lock Manager - {config.NODE_ID}", version="1.0.0")

redis_client: Optional[redis.Redis] = None
raft_node = RaftNode(config.NODE_ID, config.API_PORT)

# Pemetaan URL internal (container) -> publik (lokal) untuk redirect
INTERNAL_TO_PUBLIC_URLS = {
    "http://lock_node1:9000": "http://localhost:9000",
    "http://lock_node2:9001": "http://localhost:9001",
    "http://lock_node3:9002": "http://localhost:9002",
}


@app.on_event("startup")
async def startup():
    global redis_client
    logger.info("[%s] startup: menghubungkan ke Redis...", config.NODE_ID)

    try:
        redis_client = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
        )
        await redis_client.ping()
        logger.info("[%s] terhubung ke Redis", config.NODE_ID)
    except Exception as exc:
        logger.exception("[%s] gagal menghubungkan ke Redis: %s", config.NODE_ID, exc)
        redis_client = None

    # Jalankan timer Raft di background
    asyncio.create_task(raft_node.run_timer())
    logger.info("[%s] timer Raft dijalankan", config.NODE_ID)


@app.on_event("shutdown")
async def shutdown():
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.close()
        except Exception:
            pass
        logger.info("[%s] koneksi Redis ditutup", config.NODE_ID)


def _check_redis_client() -> None:
    if redis_client is None:
        raise HTTPException(status_code=500, detail="Redis client tidak terhubung")


def _get_leader_redirect_or_wait(request: Request) -> Optional[Response]:
    """
    Jika node bukan leader, kembalikan response redirect (307) ke leader publik.
    Jika tidak ada leader saat ini, kembalikan HTTP 503.
    """
    if raft_node.state == NodeState.LEADER:
        return None

    leader_internal_url = raft_node.current_leader_url
    if not leader_internal_url:
        raise HTTPException(status_code=503, detail="Leader sedang dipilih. Coba lagi nanti.")

    public_leader_url = INTERNAL_TO_PUBLIC_URLS.get(leader_internal_url)
    if not public_leader_url:
        logger.warning("Tidak ditemukan mapping publik untuk %s, menggunakan default", leader_internal_url)
        public_leader_url = "http://localhost:9000"

    redirect_path = request.url.path
    public_redirect_url = f"{public_leader_url}{redirect_path}"
    logger.info("[%s] meredirect ke leader: %s", config.NODE_ID, public_redirect_url)

    return JSONResponse(
        status_code=307,
        content={"detail": f"Bukan leader. Silakan coba {public_leader_url}"},
        headers={"Location": public_redirect_url},
    )


@app.get("/")
def read_root():
    return {
        "node_id": raft_node.node_id,
        "state": raft_node.state.name,
        "current_term": raft_node.current_term,
        "voted_for": raft_node.voted_for,
        "current_leader": raft_node.current_leader_url,
        "log_length": len(raft_node.log),
    }


# RPC endpoints
@app.post("/rpc/request_vote")
def rpc_request_vote(args: RequestVoteArgs):
    return raft_node.handle_request_vote(args.term, args.candidate_id)


@app.post("/rpc/append_entries")
def rpc_append_entries(args: AppendEntriesArgs):
    return raft_node.handle_append_entries(args.term, args.leader_id, args.entries)


# Client API
@app.post("/lock/acquire")
async def acquire_lock(lock_req: LockRequest, request: Request):
    _check_redis_client()

    redirect = _get_leader_redirect_or_wait(request)
    if redirect:
        return redirect

    logger.info("[%s] menerima acquire: %s", config.NODE_ID, lock_req.lock_name)

    command = {
        "op": "ACQUIRE",
        "lock_name": lock_req.lock_name,
        "client_id": lock_req.client_id,
        "timeout": lock_req.timeout_sec,
    }
    replicated = await raft_node.replicate_log_entry(command)
    if not replicated:
        raise HTTPException(status_code=503, detail="Gagal mereplikasi log ke mayoritas node")

    lock_key = f"dlock:{lock_req.lock_name}"
    was_set = await redis_client.set(lock_key, lock_req.client_id, nx=True, ex=lock_req.timeout_sec)
    if was_set:
        logger.info("[%s] lock '%s' diberikan ke %s", config.NODE_ID, lock_req.lock_name, lock_req.client_id)
        return {"status": "acquired", "lock": lock_req.lock_name, "client_id": lock_req.client_id}
    else:
        current_owner = await redis_client.get(lock_key)
        logger.info("[%s] lock '%s' sudah dipegang oleh %s", config.NODE_ID, lock_req.lock_name, current_owner)
        raise HTTPException(status_code=409, detail=f"Conflict: Lock sudah dipegang oleh {current_owner}")


@app.post("/lock/release")
async def release_lock(lock_req: LockRequest, request: Request):
    _check_redis_client()

    redirect = _get_leader_redirect_or_wait(request)
    if redirect:
        return redirect

    logger.info("[%s] menerima release: %s", config.NODE_ID, lock_req.lock_name)

    command = {"op": "RELEASE", "lock_name": lock_req.lock_name, "client_id": lock_req.client_id}
    replicated = await raft_node.replicate_log_entry(command)
    if not replicated:
        raise HTTPException(status_code=503, detail="Gagal mereplikasi log ke mayoritas node")

    lock_key = f"dlock:{lock_req.lock_name}"
    current_owner = await redis_client.get(lock_key)
    if current_owner is None:
        logger.info("[%s] lock '%s' sudah dilepas", config.NODE_ID, lock_req.lock_name)
        return {"status": "not_held", "lock": lock_req.lock_name}

    if current_owner == lock_req.client_id:
        await redis_client.delete(lock_key)
        logger.info("[%s] lock '%s' dilepas oleh %s", config.NODE_ID, lock_req.lock_name, lock_req.client_id)
        return {"status": "released", "lock": lock_req.lock_name}
    else:
        logger.info("[%s] gagal release '%s'. Pemilik saat ini: %s", config.NODE_ID, lock_req.lock_name, current_owner)
        raise HTTPException(status_code=403, detail=f"Forbidden: Anda ({lock_req.client_id}) bukan pemilik lock ({current_owner})")


if __name__ == "__main__":
    logger.info("Memulai Lock Manager %s di port %s", config.NODE_ID, config.API_PORT)
    uvicorn.run(app, host="0.0.0.0", port=config.API_PORT)
