# bdg2-anomaly — Integration Guidelines

Bổ sung cho `TASKS.md`: chuẩn kỹ thuật dùng chung + các điểm còn phải chốt giữa 4 task trước khi tích hợp. Đọc cùng `TASKS.md`, không thay thế.

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

**File dùng chung ở root:**

| File | Nội dung |
|---|---|
| `requirements-spark.txt` | pin `pyspark==4.2.0` + `delta-spark==4.4.0`; mỗi `requirements.txt` của từng task thêm `-r ../requirements-spark.txt` |
| `config.py` | `THRESHOLD_RATIO = 0.28`, `TIMEZONE = "UTC"`, `CLEAN_DATA_SCHEMA` (Task1→2,3), `ENERGY_RESULTS_SCHEMA` (Task3→4), `to_spark_struct_type()` cho bên pyspark — gộp chung 1 file để đỡ rải rác ở root |

**Import từ subfolder** (repo chưa package hóa, dùng `sys.path` nhẹ):

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root

from config import THRESHOLD_RATIO, TIMEZONE, CLEAN_DATA_SCHEMA, ENERGY_RESULTS_SCHEMA, to_spark_struct_type
```

**Hạ tầng Task 4 (docker-compose):**

| Thành phần | Chốt |
|---|---|
| Kafka | `apache/kafka:4.2.0`, KRaft, không Zookeeper (tag Bitnami `kafka:4.2`/`zookeeper` không còn trên Docker Hub, đã kiểm tra bằng `docker manifest inspect`) |
| Spark | không có container; chạy `local[*]` trên host. Xem `docs/adr/0001-kafka-kraft-no-spark-container.md` |
| Kafka listener | dual: `kafka:9092` nội bộ + `EXTERNAL` advertise `localhost:9094` |
| Kafka topic | `energy-readings`, 1 partition, tạo bởi service `kafka-init`; hằng số trong `config.py` (`KAFKA_TOPIC`, `KAFKA_BOOTSTRAP`) |
| `streaming_job.py` / `kafka_producer.py` | chạy trên host, `master=local[*]`, nối Kafka qua `localhost:9094` |
| Kafka không có volume | mỗi lần `up` là 1 phiên demo mới; reset bằng `dashboard/reset_demo.sh` (xóa luôn `streaming/output/`, gồm checkpoint) |
| Checkpoint Spark | `streaming/output/_checkpoint/` (Task 3 dùng đúng đường dẫn này) |
| Nhịp thời gian | producer 3s, Spark trigger 5s, dashboard refresh 5s (`config.py`) |
| Dashboard đọc Delta Lake | `deltalake` (delta-rs), không cần Spark — an toàn vì write pattern append-only |
| Auto-refresh dashboard | `st.fragment(run_every=...)` (Streamlit ≥1.33) |

---

## 2. Cần cả nhóm chốt

| # | Vấn đề | Rủi ro nếu bỏ qua | Cần chốt gì |
|---|---|---|---|
| 1 | Task 1 output: Parquet hay Delta? (`TASKS.md` ghi cả hai) | Task 2 load sai định dạng, lỗi ngay bước đầu | 1 định dạng duy nhất, ghi vào `TASKS.md` mục 3 |
| 2 | Danh sách `building_id` (30-50 tòa Office) chưa có nguồn công bố | Task 3 mô phỏng lệch building_id, join/lookup sai | Hưng công bố ở đâu, hay Task 3 tự đọc từ `clean_data.parquet` |
| 3 | `docker-compose.yml` cần merge sớm, khác thứ tự 1→2→3→4 ở mục 5 `TASKS.md` | Nguyên bị block chờ Task 4 dù lẽ ra làm sau | **Đã đề xuất:** PR `[infra]` (compose + config + docs) tách riêng, merge trước PR dashboard; chờ nhóm xác nhận |
| 4 | Checkpoint cho Spark Structured Streaming chưa quy ước | Restart job gây duplicate/mất bản ghi, log dashboard sai | **Đã đề xuất:** `streaming/output/_checkpoint/`, xóa cùng Kafka và Delta qua `dashboard/reset_demo.sh`; chờ Nguyên xác nhận |
| 5 | Ranh giới "injected anomaly" (Task 2, offline) vs "kịch bản tăng đột biến" (Task 3, live demo) | Trùng công sức hoặc lệch định nghĩa anomaly giữa 2 bên | Xác nhận đây là 2 việc tách biệt hay dùng chung 1 kịch bản |
| 6 | Ownership review cho file dùng chung (`config.py`, `docker-compose.yml`) | Ai cũng sửa tự do, vỡ chuẩn vừa chốt ở mục 1 | Bắt buộc bao nhiêu người review khi các file này đổi |
| 7 | Nhịp thời gian: Kafka producer (2-3s), Spark trigger interval, dashboard refresh chưa khớp nhau | Demo giật, hiển thị lệch nhịp dữ liệu thật | **Đã đề xuất:** 3s / 5s / 5s trong `config.py`; chờ nhóm xác nhận |
| 8 | Partitioning cho `clean_data.parquet` / `energy_results` chưa quy định | Đọc chậm dần khi data lớn (ảnh hưởng dashboard) | Partition theo `building_id`/ngày hay không, quyết ngay hay tối ưu sau |
| 9 | Xử lý message lỗi/trễ trong Kafka consumer chưa có quy ước | Streaming job crash hoặc âm thầm mất data | Crash, skip, hay log riêng khi gặp message sai schema/trễ |
| 10 | Chưa có runbook end-to-end cho buổi demo | Lần đầu chạy full pipeline rơi đúng hôm bảo vệ mới lộ lỗi tích hợp | Ai viết `docs/RUNBOOK.md`, viết trước deadline nào |
