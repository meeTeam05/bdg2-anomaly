# bdg2-anomaly — Integration Guidelines

Bổ sung cho `docs/TASKS.md`: chuẩn kỹ thuật dùng chung + các điểm còn phải chốt giữa 4 task trước khi tích hợp. Đọc cùng `TASKS.md`, không thay thế.

---

## 1. Đã chốt — có file thật trong repo

**Version stack (verify qua PyPI `requires_dist`, không đoán):**

| Thành phần | Version | Ghi chú |
|---|---|---|
| Python | 3.11 | ghi ở `.python-version`, thỏa cả pyspark và deltalake (yêu cầu ≥3.10) |
| Java | 17 | bắt buộc, Spark 4.x bỏ JDK 8/11 |
| pyspark | 4.2.0 | mới nhất còn trong khoảng delta-spark hỗ trợ |
| delta-spark | 4.4.0 | yêu cầu `pyspark>=4.0.1,<=4.2.0` |
| deltalake (delta-rs, Task 4 đọc) | 1.6.6 | + extra `pyarrow>=21` cho `.to_pandas()` |

**File dùng chung:**

| File | Nội dung |
|---|---|
| `pyproject.toml` | metadata project + **toàn bộ dependency** (extras `spark`, `dashboard`, `dev`) + cấu hình ruff, pytest. Thay cho `requirements-spark.txt` và các `requirements.txt` riêng |
| `common/config.py` | đường dẫn mọi tầng, tất cả nằm trong `data/` (`RAW_DATA_DIR`, `BRONZE_DIR`, `CLEAN_DATA_PATH`, `MODEL_PATH`, `ENERGY_RESULTS_PATH`, `ENERGY_RESULTS_CHECKPOINT`; đổi thư mục gốc bằng biến môi trường `BDG2_DATA_DIR`), `PARTITION_COLUMN = "site_id"`, `TEST_START`, `METER_PROFILE_PATH`, `METER_THRESHOLDS_PATH`, `ANOMALY_METER_TYPES`, `THRESHOLD_RATIO = 0.28` (dự phòng), `ANOMALY_PERSISTENCE_HOURS = 3`, `TIMEZONE = "UTC"`, `CLEAN_DATA_SCHEMA` (Task1→2,3, có `site_id`), `ENERGY_RESULTS_SCHEMA` (Task3→4), `to_spark_struct_type()` |
| `common/spark_session.py` | `build_spark(app_name, extra_packages=None)`: `local[*]`, Delta Lake, timezone UTC, driver memory 6g (đổi bằng `SPARK_DRIVER_MEMORY`). Streaming truyền Kafka connector qua `extra_packages` |
| `Makefile` | `make setup` / `data` / `lint` / `format` / `test` / `infra-up` / `infra-down` |
| `.pre-commit-config.yaml`, `.github/workflows/ci.yml` | ruff + kiểm tra file lớn khi commit; CI chạy ruff + pytest cho mỗi PR |

**Cài đặt và import** (repo là 1 package Python, không dùng `sys.path`):

```bash
make setup                  # uv tạo .venv (prompt "bdg2", Python 3.11) + cài extras + pre-commit
source .venv/bin/activate
make data                   # tải toàn bộ BDG2 vào data/raw
```

```python
from common.config import CLEAN_DATA_PATH, THRESHOLD_RATIO, CLEAN_DATA_SCHEMA, to_spark_struct_type
from common.spark_session import build_spark

spark = build_spark("train_baseline")
```

**Hạ tầng Task 4 (`infra/docker-compose.yml`):**

| Thành phần | Chốt |
|---|---|
| Zookeeper / Kafka | `bitnami/zookeeper`, `bitnami/kafka:4.2` |
| Spark | `apache/spark:4.2.0` (Bitnami hết tag Spark khả dụng) |
| Kafka listener | dual: `kafka:9092` nội bộ + `EXTERNAL` advertise `localhost:9094` |
| `streaming_job.py` / `kafka_producer.py` | chạy trên host, nối `spark://localhost:7077` + `localhost:9094` |
| Dashboard đọc Delta Lake | `deltalake` (delta-rs), không cần Spark — an toàn vì write pattern append-only |
| Auto-refresh dashboard | `st.fragment(run_every=...)` (Streamlit ≥1.33) |

---

## 2. Cần cả nhóm chốt

| # | Vấn đề | Rủi ro nếu bỏ qua | Cần chốt gì |
|---|---|---|---|
| 1 | ~~Task 1 output: Parquet hay Delta?~~ | — | **Đã chốt:** mọi bảng là Delta (Bronze/Silver/Gold), xem `docs/TASKS.md` mục 3 |
| 2 | ~~Danh sách `building_id` (30-50 tòa Office)~~ | — | **Đã chốt:** dùng toàn bộ tòa nhà; Task 3 tự đọc danh sách từ `clean_data` (giai đoạn test 2017) |
| 3 | `docker-compose.yml` cần merge sớm, khác thứ tự 1→2→3→4 ở mục 5 `docs/TASKS.md` | Nguyên bị block chờ Task 4 dù lẽ ra làm sau | Cả nhóm xác nhận: PR docker-compose merge trước, tách khỏi thứ tự chung |
| 4 | Checkpoint cho Spark Structured Streaming: vị trí đã chốt `data/checkpoints/energy_results` | Restart job gây duplicate/mất bản ghi, log dashboard sai | Giữ hay xoá checkpoint khi restart |
| 5 | Ranh giới "injected anomaly" (Task 2, offline) vs "kịch bản tăng đột biến" (Task 3, live demo) | Trùng công sức hoặc lệch định nghĩa anomaly giữa 2 bên | Xác nhận đây là 2 việc tách biệt hay dùng chung 1 kịch bản |
| 6 | Ownership review cho file dùng chung (`common/`, `infra/`, `pyproject.toml`) | Ai cũng sửa tự do, vỡ chuẩn vừa chốt ở mục 1 | Bắt buộc bao nhiêu người review khi các file này đổi |
| 7 | Nhịp thời gian: Kafka producer (2-3s), Spark trigger interval, dashboard refresh chưa khớp nhau | Demo giật, hiển thị lệch nhịp dữ liệu thật | Chốt số giây cụ thể cho cả 3 tầng |
| 8 | ~~Partitioning cho `clean_data` / `energy_results`~~ | — | **Đã chốt:** cả 2 bảng partition theo `site_id` (cột `site_id` đã thêm vào schema trong `common/config.py`) |
| 9 | Xử lý message lỗi/trễ trong Kafka consumer chưa có quy ước | Streaming job crash hoặc âm thầm mất data | Crash, skip, hay log riêng khi gặp message sai schema/trễ |
| 10 | Chưa có runbook end-to-end cho buổi demo | Lần đầu chạy full pipeline rơi đúng hôm bảo vệ mới lộ lỗi tích hợp | Ai viết `docs/RUNBOOK.md`, viết trước deadline nào |
| 11 | ~~Chỉ `electricity` hay cả 8 loại đồng hồ?~~ | — | **Đã chốt:** cả 8 loại (`METER_TYPES`), thêm cột `meter_type` vào cả 2 schema; khóa = (`building_id`, `meter_type`, `timestamp`). `solar` không dùng cho phát hiện bất thường |
| 12 | ~~Báo động giả với ngưỡng 28% cố định~~ | — | **Đã chốt:** ngưỡng riêng từng đồng hồ (`METER_THRESHOLDS_PATH`, p99 quý 1/2017) + vượt 3 giờ liền (`ANOMALY_PERSISTENCE_HOURS`); báo nhầm 29% → 4%. Task 3 phải giữ trạng thái 2 giờ trước của mỗi đồng hồ. Vài đồng hồ có mức dùng rất nhỏ/thất thường có ngưỡng rất lớn (tối đa ~7.400%) nên gần như không bao giờ cảnh báo |
