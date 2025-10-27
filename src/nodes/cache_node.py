# --- src/nodes/cache_node.py ---
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as redis
import asyncio
import json
from cachetools import LRUCache

# Impor konfigurasi
try:
    from src.utils import config
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from utils import config

# Konfigurasi cache dan kanal Pub/Sub
CACHE_MAX_SIZE = 128
CACHE_BUS_CHANNEL = "cache-bus"
MAIN_MEMORY_PREFIX = "db:main_memory:"

# State MESI (disederhanakan menjadi M, S, I)
class CacheState:
    MODIFIED = "M"
    SHARED = "S"
    INVALID = "I"

# Pydantic model untuk permintaan tulis
class CacheWriteRequest(BaseModel):
    key: str
    data: dict | str | int

app = FastAPI(
    title=f"Distributed Cache Node - {config.NODE_ID}",
    version="1.0.0"
)

# Local cache (LRU): local_cache['key'] = {'data': ..., 'state': ...}
local_cache = LRUCache(maxsize=CACHE_MAX_SIZE)

# Redis client untuk main memory dan Pub/Sub
redis_client = None
pubsub_client = None

async def pubsub_listener():
    """Listener background untuk pesan di sistem bus."""
    global local_cache, redis_client

    while redis_client is None or pubsub_client is None:
        await asyncio.sleep(0.1)

    print(f"[{config.NODE_ID}] PubSub listener aktif, subscribe ke '{CACHE_BUS_CHANNEL}'")

    try:
        ps = pubsub_client.pubsub()
        await ps.subscribe(CACHE_BUS_CHANNEL)

        async for message in ps.listen():
            if message['type'] != 'message':
                continue

            try:
                msg_data = json.loads(message['data'])
                msg_node_id = msg_data.get('node_id')

                # Abaikan pesan dari diri sendiri
                if msg_node_id == config.NODE_ID:
                    continue

                msg_type = msg_data.get('type')
                key = msg_data.get('key')
                if not key:
                    continue

                entry = local_cache.get(key)
                if not entry:
                    continue

                # Koherensi sederhana
                if msg_type == 'INVALIDATE':
                    if entry['state'] != CacheState.INVALID:
                        print(f"[{config.NODE_ID}] BUS-RX: INVALIDATE('{key}'). Set state -> I.")
                        entry['state'] = CacheState.INVALID

                elif msg_type == 'READ_REQUEST':
                    # Jika kita punya data Modified, lakukan write-back ke main memory
                    if entry['state'] == CacheState.MODIFIED:
                        print(f"[{config.NODE_ID}] BUS-RX: READ_REQUEST('{key}'). Write-back ke Redis.")
                        await redis_client.set(
                            f"{MAIN_MEMORY_PREFIX}{key}",
                            json.dumps(entry['data'])
                        )
                        entry['state'] = CacheState.SHARED

            except Exception as e:
                print(f"[{config.NODE_ID}] Error memproses pesan bus: {e}")

    except Exception as e:
        print(f"[{config.NODE_ID}] Koneksi PubSub terputus: {e}")
        asyncio.create_task(pubsub_listener())

@app.on_event("startup")
async def startup():
    """Koneksi ke Redis dan mulai listener Pub/Sub."""
    global redis_client, pubsub_client
    print(f"[{config.NODE_ID}] Startup. Menghubungkan ke Redis...")

    try:
        redis_client = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
        )
        await redis_client.ping()

        pubsub_client = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT
        )
        await pubsub_client.ping()
        print(f"[{config.NODE_ID}] Terhubung ke Redis.")
    except Exception as e:
        print(f"[{config.NODE_ID}] Gagal terhubung ke Redis: {e}")
        return

    print(f"[{config.NODE_ID}] Menjalankan PubSub listener di background...")
    asyncio.create_task(pubsub_listener())

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()
    if pubsub_client:
        await pubsub_client.close()
    print(f"[{config.NODE_ID}] Koneksi Redis ditutup.")

async def broadcast_bus_message(msg_type: str, key: str):
    """Kirim pesan ke node lain lewat Pub/Sub."""
    message = {
        'node_id': config.NODE_ID,
        'type': msg_type,
        'key': key
    }
    await pubsub_client.publish(CACHE_BUS_CHANNEL, json.dumps(message))

@app.get("/cache/read")
async def cache_read(key: str):
    """Baca data dari cache (read path)."""
    global local_cache

    entry = local_cache.get(key)

    if entry and entry['state'] != CacheState.INVALID:
        print(f"[{config.NODE_ID}] CACHE HIT: key='{key}' (State: {entry['state']})")
        return {"source": "local_cache", "state": entry['state'], "data": entry['data']}

    print(f"[{config.NODE_ID}] CACHE MISS: key='{key}'. Ambil dari main memory...")

    # Minta node lain melakukan write-back bila perlu
    await broadcast_bus_message("READ_REQUEST", key)

    # Beri jeda singkat agar write-back selesai
    await asyncio.sleep(0.05)

    data_str = await redis_client.get(f"{MAIN_MEMORY_PREFIX}{key}")
    if data_str is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' tidak ditemukan di Main Memory")

    data = json.loads(data_str)

    # Simpan di cache lokal sebagai Shared
    local_cache[key] = {'data': data, 'state': CacheState.SHARED}

    return {"source": "main_memory", "state": CacheState.SHARED, "data": data}

@app.post("/cache/write")
async def cache_write(req: CacheWriteRequest):
    """Tulis data ke cache (write path)."""
    global local_cache

    print(f"[{config.NODE_ID}] WRITE: key='{req.key}'. Set state -> M.")

    # Tulis lokal sebagai Modified
    local_cache[req.key] = {
        'data': req.data,
        'state': CacheState.MODIFIED
    }

    # Invalidate pada node lain
    await broadcast_bus_message("INVALIDATE", req.key)

    return {"status": "written_local", "state": CacheState.MODIFIED, "key": req.key}

@app.post("/cache/writeback")
async def cache_writeback(key: str):
    """Memaksa write-back dari Modified ke Main Memory."""
    entry = local_cache.get(key)
    if not entry or entry['state'] != CacheState.MODIFIED:
        raise HTTPException(status_code=400, detail="Key tidak ada atau tidak 'Modified'")

    print(f"[{config.NODE_ID}] MANUAL WRITE-BACK: key='{key}'")

    await redis_client.set(
        f"{MAIN_MEMORY_PREFIX}{key}",
        json.dumps(entry['data'])
    )
    entry['state'] = CacheState.SHARED
    return {"status": "written_to_main_memory", "state": CacheState.SHARED}

if __name__ == "__main__":
    print(f"Memulai Cache Node {config.NODE_ID} di port {config.API_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=config.API_PORT)
