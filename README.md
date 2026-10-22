# bdg2-anomaly

Giám sát và phát hiện bất thường tiêu thụ năng lượng tòa nhà thương mại theo thời gian thực bằng **Apache Kafka + Apache Spark** trên **toàn bộ** dataset [Building Data Genome 2 (BDG2)](https://github.com/buds-lab/building-data-genome-project-2).

Mục tiêu: xây dựng hoàn chỉnh 1 pipeline Big Data end-to-end bằng Spark — ingestion, làm sạch, huấn luyện model, streaming thời gian thực, dashboard.

## Kiến trúc

```
 BATCH (Spark)                                    STREAMING (Kafka + Spark)
 CSV gốc BDG2 (toàn bộ)                           Kafka Producer (replay Silver, năm 2017)
     │  data_pipeline/ingest.py                        │  streaming/kafka_producer.py
     ▼                                                 ▼
 [Bronze] Delta — dữ liệu thô                    Kafka topic energy-readings (Docker)
     │  data_pipeline/preprocess.py                    │
     ▼                                                 ▼
 [Silver] Delta — clean_data ──────────────────▶ Spark Structured Streaming
     │  ml_model/train_baseline.py (MLlib)             │  streaming/streaming_job.py
     ▼                                                 ▼
 PipelineModel + hồ sơ + ngưỡng riêng ─────────▶ [Gold] Delta — energy_results
                                                       │
                                                       ▼
                                                 Dashboard (dashboard/app.py)
```

## Cấu trúc repo

| Thư mục / file | Nội dung | Phụ trách |
|---|---|---|
| `common/` | Package dùng chung: `config.py` (đường dẫn, schema, ngưỡng), `spark_session.py` | Chung |
| `data_pipeline/` | CSV → Bronze → Silver | Hưng |
| `ml_model/` | Train + đánh giá model baseline (Spark MLlib), ngưỡng bất thường | Mai |
| `streaming/` | Kafka producer + Spark Structured Streaming → Gold | Nguyên |
| `dashboard/` | Streamlit dashboard | Nhật |
| `infra/` | `docker-compose.yml` — Kafka 4.2 KRaft dùng chung | Nhật |
| `notebooks/` | Notebook khám phá dữ liệu (EDA) | Hưng |
| `data/` | Dữ liệu thô, bảng Delta, checkpoint, model — **không commit** | — |
| `docs/` | `TASKS.md` (phân công, hợp đồng dữ liệu), `INTEGRATION_GUIDELINES.md`, `REPORT_METRICS.md` (số liệu cho báo cáo) | Chung |
| `pyproject.toml` | Dependencies (extras theo task), cấu hình ruff/pytest | Chung |

Mọi lệnh bên dưới chạy từ **gốc repo**. `make` (không tham số) liệt kê toàn bộ lệnh tắt.

---

## 1. Chuẩn bị máy (1 lần)

| Cần | Ghi chú |
|---|---|
| RAM | 16 GB (train model cuối dùng 10 GB cho Spark; streaming 6 GB) |
| Ổ đĩa | ~3 GB trống cho `data/` |
| Java 17 | Spark 4.2 chạy trên JVM: `sudo apt install openjdk-17-jdk` |
| `make`, `wget`, `git` | `sudo apt install make wget git` |
| [uv](https://docs.astral.sh/uv/) | `wget -qO- https://astral.sh/uv/install.sh \| sh` (uv tự tải Python 3.11) |
| Docker + Compose | Chỉ để chạy Kafka. Ubuntu: xem [hướng dẫn chính thức](https://docs.docker.com/engine/install/ubuntu/), sau đó `sudo usermod -aG docker $USER` rồi đăng nhập lại |

Kiểm tra: `java -version` (17), `docker run --rm hello-world`, `docker compose version`.

## 2. Cài đặt project (1 lần)

```bash
git clone https://github.com/meeTeam05/bdg2-anomaly.git && cd bdg2-anomaly
make setup                  # môi trường "bdg2" (.venv, Python 3.11) + mọi thư viện + git hooks
source .venv/bin/activate   # dòng lệnh hiện (bdg2)
```

## 3. Chạy toàn bộ pipeline lần đầu

Mỗi bước dùng đầu ra của bước trước. Thời gian đo trên máy 32 core, 16 GB RAM.

| Bước | Lệnh | Thời gian | Kết quả |
|---|---|---|---|
| 1. Tải dữ liệu | `make data` | vài phút | 10 file CSV (~370 MB) trong `data/raw/` |
| 2. Task 1: làm sạch | `make task1` | ~3-4 phút | `data/bronze/`, `data/silver/clean_data` (~45,9 triệu dòng); dòng cuối `All checks passed.` |
| 3. Task 2: model + ngưỡng | `make task2` | ~4 phút | `data/models/baseline_model`, `meter_profile`, `meter_thresholds` |
| 4. Task 3 + 4: demo | `make demo HOURS=48` | 48 tick × 3 s ≈ 2,5 phút | Gold `data/gold/energy_results` + dashboard http://localhost:8501 |

Bước 1-3 chỉ cần chạy lại khi đổi code của bước đó. Bước 4 chạy bao nhiêu lần cũng được.

**`make demo`** làm lần lượt: `make reset` (tắt Kafka, xóa Gold + checkpoint, bật lại Kafka và tạo topic) →
chạy **song song** streaming job, producer và dashboard. Ctrl+C dừng cả 3; sau đó `make infra-down` để tắt
Kafka. Bỏ `HOURS` thì replay cả năm 2017 (~7 giờ).

Muốn xem log từng phần riêng (dễ theo dõi khi demo trước hội đồng), mở 3 terminal:

```bash
make reset                 # 1 lần trước mỗi phiên demo
make stream                # terminal 1: chờ dòng "streaming energy-readings ..."
make produce HOURS=48      # terminal 2
make dashboard             # terminal 3: http://localhost:8501
```

Chỉ xem giao diện dashboard với dữ liệu giả, không cần Kafka/Spark/dữ liệu thật: `make demo-mock`.

---

## 4. Kiểm thử end-to-end

Làm theo thứ tự, mỗi tầng chỉ chạy khi tầng trước đã đạt.

### 4.1. Lint + test nhanh (không cần Kafka, ~1 phút)

```bash
make lint      # All checks passed!
make test      # ~119 passed, 14 skipped (14 test cần Kafka bị bỏ qua)
make validate  # Silver đúng hợp đồng dữ liệu: All checks passed.
```

CI trên GitHub chạy đúng `make lint` + `make test` cho mỗi lần push.

### 4.2. Test end-to-end tự động với Kafka (~2 phút)

Chạy thật `kafka_producer.py` và `streaming_job.py` dưới dạng tiến trình con, trên dữ liệu giả và topic
riêng cho mỗi lần chạy, ghi vào thư mục tạm (không đụng `data/gold`).

```bash
make infra-up    # Kafka trong Docker
make test-e2e    # lần đầu tự tạo streaming/mock_data (~1 phút) -> 14 passed
make infra-down
```

14 test này kiểm tra: mọi message đến Gold đúng 1 lần; tắt job giữa chừng rồi bật lại không mất, không
trùng; message lỗi bị bỏ và được đếm; đột biến được cảnh báo đúng từ giờ thứ 3; `batch_metrics.csv` đủ
mọi batch; compaction không làm hỏng Gold; producer báo lỗi rõ khi thiếu topic.

### 4.3. Demo thật trên toàn bộ dữ liệu (~3 phút)

```bash
make demo HOURS=48
```

Kiểm tra bằng mắt, theo thứ tự xuất hiện:

| Ở đâu | Phải thấy |
|---|---|
| Terminal | Kafka `Healthy`, `kafka-init` tạo topic; producer in tên 2 tòa nhà bị tạo đột biến |
| Terminal | Mỗi ~5 s một dòng `batch N: ~1390-2800 rows (0 invalid), ... scored, ... anomalies \| ~3s, ... rows/s \| max latency ~5-7s` |
| Dashboard | Lúc đầu "Waiting for data...", 10-15 s sau có số liệu; ~1.400 đồng hồ, 18 site |
| Dashboard | Từ khoảng tick 12, 2 tòa nhà bị tạo đột biến lên đầu bảng **Top 50 meters** với trạng thái ANOMALY, biểu đồ thực tế vượt khỏi dải ngưỡng |
| Terminal | Producer kết thúc bằng `done: ~67000 sent, 0 failed` |
| Dashboard | ~30 s sau khi producer xong (không còn dữ liệu mới): mọi đồng hồ chuyển STALE, có banner đỏ "Data source stopped sending" |

Rồi Ctrl+C để dừng và đối chiếu Gold: số dòng phải bằng số message đã gửi, không có dòng trùng.

```bash
python -c "
from deltalake import DeltaTable
g = DeltaTable('data/gold/energy_results').to_pandas()
print(len(g), 'rows,', g.duplicated(['building_id','meter_type','timestamp']).sum(), 'duplicates,',
      g.groupby(['building_id','meter_type']).ngroups, 'meters,', f'{g.is_anomaly.mean():.1%} anomalies')"
make infra-down
```

Kết quả tham khảo (48 giờ): `66997 rows, 0 duplicates, 1403 meters, 2.2% anomalies`. Throughput và độ
trễ từng batch nằm trong `data/gold/batch_metrics.csv` (số liệu đầy đủ ở `streaming/README.md`).

### 4.4. Restart giữa chừng (exactly-once, tùy chọn)

Mở 3 terminal như mục 3 với `make produce` (cả năm). Đang chạy thì Ctrl+C **chỉ** terminal `make stream`,
chờ 20 s, chạy lại `make stream`: job đọc tiếp từ checkpoint, đuổi kịp producer, và lệnh đối chiếu ở 4.3
vẫn cho `0 duplicates`.

---

## 5. Sự cố thường gặp

| Triệu chứng | Cách xử lý |
|---|---|
| `permission denied ... docker.sock` | Chưa vào nhóm `docker`: `sudo usermod -aG docker $USER`, đăng nhập lại (hoặc `newgrp docker`) |
| `container name "/bdg2-kafka" is already in use` | Có 2 lệnh `make demo`/`infra-up` chạy cùng lúc; chỉ chạy 1 lần, hoặc `make infra-down` rồi chạy lại |
| `Topic 'energy-readings' does not exist` | Kafka không được bật bằng `infra/docker-compose.yml` (service `kafka-init` tạo topic): `make reset` |
| Streaming job lỗi offset sau khi tắt/bật Kafka | Kafka không có volume, checkpoint còn của phiên cũ: `make reset` |
| `Model not found` / `Meter profile not found` | Chưa chạy `make task2` |
| `java.lang.OutOfMemoryError` khi train | Tăng/giảm RAM Spark: `make task2 SPARK_DRIVER_MEMORY=12g`; không chạy 2 job Spark cùng lúc |
| Port 8501 đang bận | Dashboard cũ còn chạy: tắt nó, hoặc `streamlit run dashboard/app.py --server.port 8502` |
| Lần chạy streaming đầu tiên chậm | Spark tải jar Kafka + Delta (cần mạng, chỉ 1 lần) |

Chi tiết từng phần xem `README.md` trong mỗi thư mục (`streaming/README.md` có thêm sự cố riêng cho
Windows).

## 6. Phát triển

```bash
make lint     # ruff kiểm tra PEP 8
make format   # ruff tự sửa và định dạng
make test     # pytest (không cần Kafka)
```

Quy tắc làm việc nhóm, hợp đồng dữ liệu và quy ước commit xem [`docs/TASKS.md`](docs/TASKS.md).
