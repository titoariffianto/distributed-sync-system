# ⚡ Distributed System Demo (Queue | Lock | Cache)

Proyek ini menampilkan implementasi tiga komponen sistem terdistribusi berbasis **FastAPI** dan **Docker**:
- 📨 **Distributed Queue**
- 🔐 **Distributed Lock Manager (Raft Consensus)**
- 💾 **Distributed Cache (MESI Protocol)**

Semua layanan dijalankan melalui **Docker Compose** untuk memudahkan pengujian lokal.

---

## 🧰 Prasyarat
- Sudah terinstal **Docker** dan **Docker Compose**

---

## 🚀 Menjalankan Sistem

```bash
git clone https://github.com/<username>/<repo-name>.git
cd <repo-name>
docker-compose up --build
```

Setelah container aktif, buka dokumentasi API:
- Swagger: [http://localhost:<port>/docs](http://localhost:<port>/docs)
- Redoc: [http://localhost:<port>/redoc](http://localhost:<port>/redoc)

---

## 🧪 Uji Manual Komponen

### 📨 Queue System
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/enqueue" -Body '{"message":"Halo dari node 1"}' -ContentType "application/json"
Invoke-RestMethod -Method Post -Uri "http://localhost:8001/enqueue" -Body '{"message":"Tes dari node 2"}' -ContentType "application/json"
Invoke-RestMethod -Method Get -Uri "http://localhost:8000/dequeue"
Invoke-RestMethod -Method Get -Uri "http://localhost:8001/dequeue"
```

### 💾 Cache Coherence (MESI)
```powershell
docker-compose exec redis redis-cli SET "db:main_memory:user:123" '{"name": "Data Awal", "value": 0}'
$body1 = '{"key":"user:123","data":{"name":"Data Node 1","value":42}}'
Invoke-RestMethod -Method Post -Uri "http://localhost:7000/cache/write" -Body $body1 -ContentType "application/json"
Invoke-RestMethod -Method Get -Uri "http://localhost:7001/cache/read?key=user:123"
Invoke-RestMethod -Method Get -Uri "http://localhost:7002/cache/read?key=user:123"
Invoke-RestMethod -Method Get -Uri "http://localhost:7001/cache/read?key=user:123"
```

### 🔐 Lock Manager (Raft)
```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:9000/"
Invoke-RestMethod -Method Post -Uri "http://localhost:9000/lock/acquire" -Body '{"lock_name":"demo","client_id":"c1","timeout_sec":5}' -ContentType "application/json"
Invoke-RestMethod -Method Post -Uri "http://localhost:9000/lock/release" -Body '{"lock_name":"demo","client_id":"c1"}' -ContentType "application/json"
```

---

## 📊 Pengujian Beban

```bash
pip install locust uhashring
locust -f benchmarks/load_test_scenarios.py
```

Akses dashboard: [http://localhost:8089](http://localhost:8089)

---

## 🛠️ Troubleshooting Cepat

| Masalah | Penyebab | Solusi |
|----------|-----------|--------|
| Tidak bisa melihat log | Belum tahu cara melihat log per node | `docker-compose logs -f <service>` |
| Node gagal hubung Redis | Redis belum siap | Tambahkan `depends_on` & `healthcheck`, lalu `docker-compose down && up` |
| Raft tidak pilih leader | Salah konfigurasi host | Pastikan semua `lock_node*` di jaringan sama |
| Invoke-RestMethod gagal | Container belum aktif | Jalankan ulang `docker-compose up` dan cek `docker ps` |

---

📄 **Lisensi:** MIT License  
👤 Pengembang: *Tito Ariffianto Miftahul Huda*
