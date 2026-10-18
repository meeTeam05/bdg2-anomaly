# streaming — Task 3 (Nguyên)

Kafka producer replay dữ liệu Silver (năm 2017) → Spark Structured Streaming → **Gold** (Delta Lake).

```
Silver (Delta) ──kafka_producer.py──▶ Kafka topic energy-readings ──streaming_job.py──▶ Gold (Delta)
 19 thread, 1 thread/site, 3s/tick        JSON, key = building_id      model → baseline, residual, is_anomaly
```

| | Đường dẫn / hằng số (`config.py`) |
|---|---|
| Input dữ liệu replay | `CLEAN_DATA_PATH`, chỉ `timestamp >= TEST_START` |
| Input model | `MODEL_PATH` (PipelineModel của Task 2, output cột `prediction`) |
| Kafka | `KAFKA_BOOTSTRAP` = `localhost:9094`, `KAFKA_TOPIC` = `energy-readings` |
| Nhịp | `PRODUCER_INTERVAL_SEC` = 3, `SPARK_TRIGGER_SEC` = 5 |
| Output | `ENERGY_RESULTS_PATH` = `streaming/output/energy_results`, partition `site_id`, schema `ENERGY_RESULTS_SCHEMA` |
| Checkpoint | `ENERGY_RESULTS_CHECKPOINT` = `streaming/output/_checkpoints/energy_results` |
| Số liệu từng batch | `streaming/output/batch_metrics.csv` |

## File

| File | Làm gì |
|---|---|
| `kafka_producer.py` | Đọc Silver theo từng site (`deltalake`), mỗi tick gửi tất cả tòa nhà của site tại cùng 1 giờ. Tạo đột biến cho live demo. |
| `streaming_job.py` | Đọc Kafka → parse JSON → bỏ message lỗi → model → `baseline`, `residual`, `is_anomaly` → append Gold. Log + CSV mỗi batch. |
| `spark_session.py` | SparkSession dùng chung: `local[*]`, Delta, Kafka connector, UTC, các chỉnh cho Windows. |
| `mock_inputs.py` | Tạo Silver + model giả (đúng schema, đúng dạng Pipeline của Task 2) để chạy khi Task 1/2 chưa xong. |

## Cài đặt

```bash
pip install -r requirements.txt     # Python 3.11, Java 17
```

**Windows:** Spark cần `winutils.exe` + `hadoop.dll` (bản Hadoop 3.4, ví dụ từ `kontext-tech/winutils`)
đặt trong `C:\hadoop\bin`, rồi `setx HADOOP_HOME C:\hadoop` và thêm `%HADOOP_HOME%\bin` vào `PATH`.
Thiếu thì script dừng ngay với thông báo rõ ràng.

## Chạy

Mọi lệnh chạy trong thư mục `streaming/`, Kafka phải đang chạy:

```bash
docker compose -f ../dashboard/docker-compose.yml up -d --wait
```

**Với dữ liệu thật** (cần Silver của Task 1 và model của Task 2):

```bash
python streaming_job.py                 # terminal 1, chờ dòng "streaming energy-readings ..."
python kafka_producer.py --hours 48     # terminal 2, 48 giờ của 2017 = 48 tick x 3s
```

**Với dữ liệu giả** (khi Task 1/2 chưa bàn giao):

```bash
python mock_inputs.py                                         # 1 lần, ~1 phút
python streaming_job.py --model mock_data/baseline_model
python kafka_producer.py --silver mock_data/clean_data --hours 30
```

Tùy chọn hay dùng (xem `--help`):

| Lệnh | Tùy chọn |
|---|---|
| `kafka_producer.py` | `--hours N` (mặc định cả năm 2017), `--start`, `--interval`, `--sites A B`, `--spike-count 2`, `--spike-after 10`, `--spike-factor 3`, `--verbose` |
| `streaming_job.py` | `--model`, `--log-target` (model train trên `log1p`), `--trigger 5`, `--compact-every 60` |

**Chạy lại từ đầu** (Kafka không có volume, nên checkpoint và Gold phải xóa cùng lúc):

```bash
docker compose -f ../dashboard/docker-compose.yml down
rm -rf output/                      # Gold + checkpoint + batch_metrics.csv
docker compose -f ../dashboard/docker-compose.yml up -d --wait
```

## Quy ước xử lý

- **`baseline = max(prediction, 0)`**: regression có thể dự đoán số kWh âm, khi đó `is_anomaly` sẽ luôn đúng.
  Nếu model train trên `log1p(meter_reading)` thì chạy với `--log-target` (job sẽ áp `expm1`).
- **`is_anomaly = abs(residual) > baseline * THRESHOLD_RATIO`**, ngưỡng import từ `config.py`.
  Tòa nhà có baseline gần 0 sẽ báo động liên tục (thấy rõ ở `Panther_public_Tiny_Kiosk` trong mock).
  Ngưỡng tối thiểu do Task 2 đề xuất (`INTEGRATION_GUIDELINES.md` mục 2, #12).
- **Message lỗi** (không phải JSON, thiếu field, sai kiểu): không làm job crash, bị bỏ qua và được đếm ở cột `invalid` trong log/CSV.
- **Exactly-once**: Delta sink + checkpoint. Restart job (giữ `output/`) sẽ chạy tiếp từ offset cũ, không ghi trùng.
- **Live demo spike** (khác injected-anomaly offline của Task 2): `--spike-count` tòa nhà tiêu thụ lớn nhất
  nhân `meter_reading` với `--spike-factor` từ tick `--spike-after`. Tên các tòa nhà này được in ra khi chạy.
- **File nhỏ**: mỗi `--compact-every` batch job chạy `OPTIMIZE` để dashboard đọc nhanh.

## Đọc log

```
batch 14: 24 rows (0 invalid), 11 anomalies | 2.66s, 9 rows/s | max latency 7.1s
```

`rows/s` = số dòng / thời gian xử lý batch. `max latency` = từ lúc producer gửi bản ghi cũ nhất của batch
đến lúc batch ghi xong vào Gold (gồm cả thời gian chờ trigger).

## Sự cố thường gặp

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| `HADOOP_HOME is not set` | Windows thiếu winutils, xem mục Cài đặt |
| `Kafka not reachable` / `Topic ... does not exist` | Chưa `docker compose up`; topic do service `kafka-init` tạo |
| Job lỗi offset sau khi `docker compose down/up` | Kafka đã mới nhưng checkpoint còn cũ → xóa `output/` |
| `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` lúc tắt | Lỗi vô hại trên Windows (file bị khóa), bỏ qua |
| Dashboard thấy dữ liệu cũ | `output/` của lần chạy trước còn đó → reset như trên |

## Số liệu quy mô

| | Mock (12 tòa nhà, 3 site) | Dữ liệu thật |
|---|---|---|
| Bản ghi / tick | 12 | TODO |
| Thời gian xử lý 1 batch | 2–3 s | TODO |
| Độ trễ tối đa (gửi → Gold) | 4–10 s | TODO |
| Throughput (bản ghi/giây) | ~4–12 (giới hạn bởi nhịp producer) | TODO |

Cấu hình Spark: `local[*]`, `spark.driver.memory=4g`, `spark.sql.shuffle.partitions=8`,
`spark.databricks.delta.snapshotPartitions=4`, trigger 5s. Máy: Windows 11, Java 17, pyspark 4.2.0, delta-spark 4.4.0.
