# streaming — Task 3 (Nguyên)

Kafka producer replay dữ liệu Silver (năm 2017) → Spark Structured Streaming → **Gold** (Delta Lake).

| | Đường dẫn (`config.py`) |
|---|---|
| Input dữ liệu replay | `CLEAN_DATA_PATH`, chỉ `timestamp >= TEST_START` |
| Input model | `MODEL_PATH` |
| Output | `ENERGY_RESULTS_PATH` = `streaming/output/energy_results`, partition theo `site_id`, schema `ENERGY_RESULTS_SCHEMA` |
| Checkpoint | `ENERGY_RESULTS_CHECKPOINT` |

- Producer: mỗi site 1 thread, mỗi tick (2–3s) gửi lô bản ghi của tất cả tòa nhà trong site.
- `is_anomaly = abs(residual) > baseline * THRESHOLD_RATIO`.

## Cách chạy

```bash
docker compose -f ../dashboard/docker-compose.yml up -d
pip install -r requirements.txt
python streaming_job.py
python kafka_producer.py
```

## Số liệu quy mô

Throughput (bản ghi/giây): TODO. Độ trễ micro-batch: TODO. Cấu hình Spark: TODO
