# /D:/distributed-sync-system/benchmarks/load_test_scenarios.py
import random
import string
import time

from locust import HttpUser, task, between, SequentialTaskSet
from uhashring import HashRing

# Konfigurasi consistent hashing untuk queue
NODES = {
    "node1": "http://localhost:8000",
    "node2": "http://localhost:8001",
    "node3": "http://localhost:8002",
}
RING = HashRing(nodes=list(NODES.keys()))

# Node untuk pengujian lock manager
LOCK_NODES = [
    "http://localhost:9000",
    "http://localhost:9001",
    "http://localhost:9002",
]


def random_string(length: int = 10) -> str:
    """Buat string acak untuk key/message id."""
    return "".join(random.choice(string.ascii_letters) for _ in range(length))


class DistributedQueueUser(HttpUser):
    """Simulasikan producer + consumer terhadap queue terdistribusi."""
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.node_id = random.choice(list(NODES.keys()))
        self.host = NODES[self.node_id]
        print(f"User baru dibuat. Menargetkan {self.host} untuk dequeue.")

    @task(10)
    def enqueue_message(self):
        """Mensimulasikan producer: enqueue pesan ke node sesuai hash key."""
        message_key = f"key-{random_string(8)}"
        message_data = f"data_untuk_{message_key}"

        target_node_id = RING.get_node(message_key)
        target_host = NODES[target_node_id]

        with self.client.session.post(
            f"{target_host}/enqueue",
            json={"message": message_data},
            name="/enqueue (hashed)",
            catch_response=True,
        ) as response:
            if not response.ok:
                response.failure(f"Gagal enqueue ke {target_host}")
            else:
                response.success()

    @task(5)
    def dequeue_and_ack(self):
        """Mensimulasikan consumer: dequeue dari node yang dipilih pada start lalu ack jika ada pesan."""
        message_data = None
        with self.client.get(
            f"{self.host}/dequeue",
            name="/dequeue",
            catch_response=True,
        ) as response:
            if response.ok:
                try:
                    data = response.json()
                    if data.get("status") == "dequeued":
                        message_data = data.get("message")
                except Exception:
                    response.failure("Response JSON tidak valid")
            else:
                response.failure("Gagal dequeue")

        if message_data:
            self.client.post(
                f"{self.host}/ack",
                json={"message": message_data},
                name="/ack",
            )


class LockTaskSequence(SequentialTaskSet):
    """Sequence tugas untuk menguji lock manager (acquire -> hold -> release)."""

    def on_start(self):
        self.client_id = f"client-{random_string(6)}"
        self.lock_name = "test-lock-1"
        self.acquired_lock = False

    @task
    def acquire_lock(self):
        """Coba acquire lock; catat status berdasarkan kode respons."""
        print(f"[{self.client_id}] Mencoba acquire '{self.lock_name}'...")
        with self.client.post(
            "/lock/acquire",
            json={"lock_name": self.lock_name, "client_id": self.client_id, "timeout_sec": 10},
            catch_response=True,
            name="/lock/acquire",
        ) as response:
            if response.status_code == 200:
                print(f"[{self.client_id}] BERHASIL acquire '{self.lock_name}'")
                response.success()
                self.acquired_lock = True
            elif response.status_code == 409:
                print(f"[{self.client_id}] GAGAL acquire (sudah dipegang)")
                response.success()
                self.acquired_lock = False
            elif response.status_code == 307:
                print(f"[{self.client_id}] Di-redirect ke leader...")
                response.success()
                self.acquired_lock = False
            else:
                print(f"[{self.client_id}] Error: {response.status_code} - {response.text}")
                response.failure(f"Status code: {response.status_code}")
                self.acquired_lock = False

    @task
    def hold_lock(self):
        """Jika memegang lock, lakukan simulasi pekerjaan singkat."""
        if not getattr(self, "acquired_lock", False):
            return
        print(f"[{self.client_id}] ... Memegang lock ...")
        time.sleep(random.uniform(0.5, 1.5))

    @task
    def release_lock(self):
        """Lepaskan lock jika dimiliki, lalu hentikan sequence (interrupt) agar ulang dari awal."""
        if not getattr(self, "acquired_lock", False):
            self.interrupt()
            return

        print(f"[{self.client_id}] Melepas lock '{self.lock_name}'...")
        self.client.post(
            "/lock/release",
            json={"lock_name": self.lock_name, "client_id": self.client_id, "timeout_sec": 10},
            name="/lock/release",
        )
        self.acquired_lock = False
        self.interrupt()


class LockUser(HttpUser):
    """User yang menjalankan LockTaskSequence terhadap salah satu node lock secara acak."""
    tasks = [LockTaskSequence]
    wait_time = between(0.5, 1.0)

    def on_start(self):
        self.host = random.choice(LOCK_NODES)
        print(f"LockUser baru. Menargetkan {self.host}")
