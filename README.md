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
 PipelineModel (baseline_model) ───────────────▶ Dashboard (dashboard/dashboard.py)
```

## Cấu trúc repo

| Thư mục | Nội dung | Phụ trách |
|---|---|---|
| `data_pipeline/` | CSV → Bronze → Silver | Hưng |
| `ml_model/` | Train + đánh giá model baseline (Spark MLlib) | Mai |
| `streaming/` | Kafka producer + Spark Structured Streaming → Gold | Nguyên |
| `dashboard/` | Streamlit dashboard + `docker-compose.yml` (Kafka, Zookeeper, Spark) | Nhật |
| `docs/` | Báo cáo, slide, `INTEGRATION_GUIDELINES.md` | Chung |
| `config.py` | Đường dẫn các bảng, schema, ngưỡng anomaly | Chung |

## Chạy end-to-end

Yêu cầu: Python 3.11, Java 17, Docker. Version thư viện xem `requirements-spark.txt`.

1. Tải BDG2 (`electricity.csv`, `weather.csv`, `metadata.csv`) vào `data_pipeline/raw/`.
2. `docker compose -f dashboard/docker-compose.yml up -d`
3. `python data_pipeline/ingest.py` → Bronze
4. `python data_pipeline/preprocess.py` → Silver
5. `python ml_model/train_baseline.py` → model
6. `python streaming/streaming_job.py` và `python streaming/kafka_producer.py` → Gold
7. `streamlit run dashboard/dashboard.py` → http://localhost:8501

Chi tiết từng bước xem `README.md` trong mỗi thư mục. Phân công và hợp đồng dữ liệu xem `TASKS.md`.
