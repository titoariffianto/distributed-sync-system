# src/nodes/queue_node.py
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as redis
import asyncio

# Muat konfigurasi
try:
    from src.utils import config
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from utils import config


app = FastAPI(
    title=f"Distributed Queue Node - {config.NODE_ID}",
    version="1.0.0"
)
redis_client = None

# Pydantic models
class MessageIn(BaseModel):
    message: str

class AckIn(BaseModel):
    message: str

# Koneksi Redis saat startup
@app.on_event("startup")
async def startup():
    global redis_client
    print(f"[{config.NODE_ID}] Menghubungkan ke Redis di {config.REDIS_HOST}:{config.REDIS_PORT}")
    
    # Coba beberapa kali jika Redis belum siap
    for _ in range(5):
        try:
            redis_client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                decode_responses=True
            )
            await redis_client.ping()
            print(f"[{config.NODE_ID}] Terhubung ke Redis.")
            return
        except redis.exceptions.ConnectionError as e:
            print(f"[{config.NODE_ID}] Menunggu Redis... ({e})")
            await asyncio.sleep(2)
    
    print(f"[{config.NODE_ID}] Gagal terhubung ke Redis setelah beberapa percobaan.")
    exit(1)

# Tutup koneksi Redis saat shutdown
@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()
        print(f"[{config.NODE_ID}] Koneksi Redis ditutup.")

# API endpoints
@app.get("/")
def read_root():
    return {"node_id": config.NODE_ID, "status": "running"}

@app.post("/enqueue")
async def enqueue(item: MessageIn):
    """
    Tambah pesan ke antrian Redis node ini.
    """
    queue_name = f"queue:{config.NODE_ID}"
    try:
        await redis_client.lpush(queue_name, item.message)
        return {"status": "enqueued", "node": config.NODE_ID, "queue": queue_name, "message": item.message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dequeue")
async def dequeue():
    """
    Ambil pesan dari antrian (non-blocking).
    Memindahkan pesan ke daftar 'processing' untuk pemrosesan.
    """
    queue_name = f"queue:{config.NODE_ID}"
    processing_name = f"processing:{config.NODE_ID}"
    
    try:
        # Gunakan rpoplpush agar operasi tidak blocking
        message = await redis_client.rpoplpush(queue_name, processing_name)
        
        if message is None:
            return {"status": "empty", "node": config.NODE_ID}
        
        return {"status": "dequeued", "node": config.NODE_ID, "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ack")
async def ack(item: AckIn):
    """
    Konfirmasi bahwa pesan selesai diproses.
    Pesan dihapus dari daftar 'processing'.
    """
    processing_name = f"processing:{config.NODE_ID}"
    try:
        removed_count = await redis_client.lrem(processing_name, 1, item.message)
        
        if removed_count == 0:
            raise HTTPException(status_code=404, detail="Message not found in processing queue or already acknowledged")
        
        return {"status": "acknowledged", "node": config.NODE_ID, "message": item.message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print(f"Memulai node {config.NODE_ID} di port {config.API_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=config.API_PORT)
