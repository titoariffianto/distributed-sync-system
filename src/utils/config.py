# --- src/utils/config.py ---
import os
from dotenv import load_dotenv

# Muat variabel lingkungan dari file .env di root proyek
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)

# Konfigurasi Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Konfigurasi Node (Queue & Lock)
NODE_ID = os.getenv("NODE_ID", "default-node")
API_PORT = int(os.getenv("API_PORT", 8000))

# Konfigurasi Raft
# Daftar node Raft (URL) dipisahkan koma.
# Contoh: "http://lock_node1:9000,http://lock_node2:9001,http://lock_node3:9002"
RAFT_NODES_STR = os.getenv("RAFT_NODES", "")
RAFT_NODES = RAFT_NODES_STR.split(',') if RAFT_NODES_STR else []

# Hitung jumlah mayoritas untuk Raft
RAFT_MAJORITY = (len(RAFT_NODES) // 2) + 1