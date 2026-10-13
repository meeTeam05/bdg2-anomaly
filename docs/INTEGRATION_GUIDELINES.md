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
| `config.py` | đường dẫn mọi tầng (`RAW_DATA_DIR`, `BRONZE_DIR`, `CLEAN_DATA_PATH`, `MODEL_PATH`, `ENERGY_RESULTS_PATH`, `ENERGY_RESULTS_CHECKPOINT`), `PARTITION_COLUMN = "site_id"`, `TEST_START`, `THRESHOLD_RATIO = 0.28`, `TIMEZONE = "UTC"`, `CLEAN_DATA_SCHEMA` (Task1→2,3, có `site_id`), `ENERGY_RESULTS_SCHEMA` (Task3→4), `to_spark_struct_type()` cho bên pyspark — gộp chung 1 file để đỡ rải rác ở root |

**Import từ subfolder** (repo chưa package hóa, dùng `sys.path` nhẹ):

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root

from config import CLEAN_DATA_PATH, THRESHOLD_RATIO, CLEAN_DATA_SCHEMA, to_spark_struct_type
```

**Hạ tầng Task 4 (docker-compose):**

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
| 1 | ~~Task 1 output: Parquet hay Delta?~~ | — | **Đã chốt:** mọi bảng là Delta (Bronze/Silver/Gold), xem `TASKS.md` mục 3 |
| 2 | ~~Danh sách `building_id` (30-50 tòa Office)~~ | — | **Đã chốt:** dùng toàn bộ tòa nhà; Task 3 tự đọc danh sách từ `clean_data` (giai đoạn test 2017) |
| 3 | `docker-compose.yml` cần merge sớm, khác thứ tự 1→2→3→4 ở mục 5 `TASKS.md` | Nguyên bị block chờ Task 4 dù lẽ ra làm sau | Cả nhóm xác nhận: PR docker-compose merge trước, tách khỏi thứ tự chung |
| 4 | Checkpoint cho Spark Structured Streaming: vị trí đã chốt `streaming/output/_checkpoints/energy_results` | Restart job gây duplicate/mất bản ghi, log dashboard sai | Giữ hay xoá checkpoint khi restart |
| 5 | Ranh giới "injected anomaly" (Task 2, offline) vs "kịch bản tăng đột biến" (Task 3, live demo) | Trùng công sức hoặc lệch định nghĩa anomaly giữa 2 bên | Xác nhận đây là 2 việc tách biệt hay dùng chung 1 kịch bản |
| 6 | Ownership review cho file dùng chung (`config.py`, `docker-compose.yml`) | Ai cũng sửa tự do, vỡ chuẩn vừa chốt ở mục 1 | Bắt buộc bao nhiêu người review khi các file này đổi |
| 7 | Nhịp thời gian: Kafka producer (2-3s), Spark trigger interval, dashboard refresh chưa khớp nhau | Demo giật, hiển thị lệch nhịp dữ liệu thật | Chốt số giây cụ thể cho cả 3 tầng |
| 8 | ~~Partitioning cho `clean_data` / `energy_results`~~ | — | **Đã chốt:** cả 2 bảng partition theo `site_id` (cột `site_id` đã thêm vào schema trong `config.py`) |
| 9 | Xử lý message lỗi/trễ trong Kafka consumer chưa có quy ước | Streaming job crash hoặc âm thầm mất data | Crash, skip, hay log riêng khi gặp message sai schema/trễ |
| 10 | Chưa có runbook end-to-end cho buổi demo | Lần đầu chạy full pipeline rơi đúng hôm bảo vệ mới lộ lỗi tích hợp | Ai viết `docs/RUNBOOK.md`, viết trước deadline nào |
| 11 | "Dùng toàn bộ dataset" chỉ gồm đồng hồ `electricity` hay cả 8 loại đồng hồ BDG2 (chilledwater, steam, gas, …) | Hiểu sai yêu cầu của GV; nếu thêm loại đồng hồ thì phải đổi schema | Hỏi GV; nếu cần nhiều loại thì thêm cột `meter_type` vào schema mục 3 |
| 12 | Tòa nhà tiêu thụ rất nhỏ (baseline ≈ 0) dễ vượt ngưỡng 28% liên tục | Dashboard ngập báo động giả | Task 2 đo tỉ lệ báo động giả, đề xuất ngưỡng tối thiểu tuyệt đối nếu cần |
