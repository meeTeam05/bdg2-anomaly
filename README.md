# bdg2-anomaly

Giám sát và phát hiện bất thường tiêu thụ năng lượng tòa nhà thương mại theo thời gian thực bằng **Apache Kafka + Apache Spark** trên **toàn bộ** dataset [Building Data Genome 2 (BDG2)](https://github.com/buds-lab/building-data-genome-project-2).

Mục tiêu: xây dựng hoàn chỉnh 1 pipeline Big Data end-to-end bằng Spark — ingestion, làm sạch, huấn luyện model, streaming thời gian thực, dashboard.

## Kiến trúc

```
 BATCH (Spark)                                    STREAMING (Kafka + Spark)
 CSV gốc BDG2 (toàn bộ)                           Kafka Producer (replay Silver, năm 2017)
     │  data_pipeline/ingest.py                        │
     ▼                                                 ▼
 [Bronze] Delta — dữ liệu thô                    Spark Structured Streaming
     │  data_pipeline/preprocess.py                    │  streaming/streaming_job.py
     ▼                                                 ▼
 [Silver] Delta — clean_data                     [Gold] Delta — energy_results
     │  ml_model/train_baseline.py (MLlib)             │
     ▼                                                 ▼
 PipelineModel (baseline_model) ───────────────▶ Dashboard (dashboard/app.py)
```

## Cấu trúc repo

| Thư mục / file | Nội dung | Phụ trách |
|---|---|---|
| `common/` | Package dùng chung: `config.py` (đường dẫn, schema, ngưỡng), `spark_session.py` | Chung |
| `data_pipeline/` | CSV → Bronze → Silver | Hưng |
| `ml_model/` | Train + đánh giá model baseline (Spark MLlib) | Mai |
| `streaming/` | Kafka producer + Spark Structured Streaming → Gold | Nguyên |
| `dashboard/` | Streamlit dashboard | Nhật |
| `infra/` | `docker-compose.yml` — hạ tầng dùng chung (Kafka KRaft) | Nhật |
| `notebooks/` | Notebook khám phá dữ liệu (EDA) | Hưng |
| `data/` | Dữ liệu thô, bảng Delta, checkpoint, model — **không commit** | — |
| `docs/` | `TASKS.md` (phân công, hợp đồng dữ liệu), `INTEGRATION_GUIDELINES.md`, báo cáo | Chung |
| `pyproject.toml` | Dependencies (extras theo task), cấu hình ruff/pytest | Chung |

## Cài đặt

Yêu cầu: Java 17, Docker, `make`, `wget`, [uv](https://docs.astral.sh/uv/) (`uv` tự tải Python 3.11).

```bash
make setup                  # tạo môi trường "bdg2" (.venv, Python 3.11) + cài thư viện + git hooks
source .venv/bin/activate   # dòng lệnh hiện (bdg2)
make data                   # tải toàn bộ BDG2 (~370 MB) vào data/raw/
```

## Chạy end-to-end

```bash
make task1                             # Task 1: CSV -> Bronze -> Silver -> kiểm tra (~3-4 phút)
SPARK_DRIVER_MEMORY=10g python ml_model/train_baseline.py --save   # Task 2: model + hồ sơ
SPARK_DRIVER_MEMORY=10g python ml_model/evaluate.py --save meter_p99_3h   # ngưỡng riêng
make demo HOURS=48                     # Kafka + streaming + producer + dashboard, http://localhost:8501
```

`make demo` luôn bắt đầu phiên mới (`make reset`) rồi chạy song song 3 phần, Ctrl+C dừng tất cả (sau đó
`make infra-down` để tắt Kafka). Bỏ `HOURS` thì replay cả năm 2017 (~7 giờ). Muốn xem log từng phần riêng:
`make infra-up`, rồi mỗi terminal một lệnh `make stream` / `make produce HOURS=48` / `make dashboard`.

Chạy lại demo từ đầu: `make reset` (Kafka, bảng Gold và checkpoint phải xóa cùng lúc). Chỉ xem
dashboard với dữ liệu giả, không cần Kafka/Spark: `make demo-mock`.

## Phát triển

```bash
make lint     # ruff kiểm tra PEP 8
make format   # ruff tự sửa và định dạng
make test     # pytest
```

Quy tắc làm việc nhóm, hợp đồng dữ liệu và quy ước commit xem [`docs/TASKS.md`](docs/TASKS.md). Chi tiết từng phần xem `README.md` trong mỗi thư mục.
