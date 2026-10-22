# streaming — Task 3 (Nguyên)

Kafka producer replay dữ liệu Silver (năm 2017) → Spark Structured Streaming → **Gold** (Delta Lake).

```
Silver (Delta) ──kafka_producer.py──▶ Kafka topic energy-readings ──streaming_job.py──▶ Gold (Delta)
 1 thread/site, 3s/tick, chỉ điện     JSON, key = building_id      hồ sơ + model → baseline, residual
                                                                   → ngưỡng riêng + 3 giờ liền → is_anomaly
```

| | Đường dẫn / hằng số (`common/config.py`) |
|---|---|
| Input dữ liệu replay | `CLEAN_DATA_PATH`, chỉ `timestamp >= TEST_START` và `ANOMALY_METER_TYPES` (điện) |
| Input từ Task 2 | `MODEL_PATH` (PipelineModel, cột `prediction`), `METER_PROFILE_PATH` (hồ sơ đồng hồ), `METER_THRESHOLDS_PATH` (ngưỡng riêng) |
| Quy tắc bất thường | `abs(residual) > baseline * anomaly_threshold` liên tục `ANOMALY_PERSISTENCE_HOURS` (3) giờ; thiếu ngưỡng → `THRESHOLD_RATIO` |
| Kafka | `KAFKA_BOOTSTRAP` = `localhost:9094`, `KAFKA_TOPIC` = `energy-readings` |
| Nhịp | `PRODUCER_INTERVAL_SEC` = 3, `SPARK_TRIGGER_SEC` = 5 |
| Output | `ENERGY_RESULTS_PATH` = `data/gold/energy_results`, partition `site_id`, schema `ENERGY_RESULTS_SCHEMA` |
| Checkpoint | `ENERGY_RESULTS_CHECKPOINT` = `data/checkpoints/energy_results` |
| Số liệu từng batch | `data/gold/batch_metrics.csv` |

## File

| File | Làm gì |
|---|---|
| `kafka_producer.py` | Đọc Silver theo từng site (`deltalake`, không cần JVM), chỉ đồng hồ điện; mỗi tick gửi mọi đồng hồ của site tại cùng 1 giờ. Tạo đột biến cho live demo. |
| `streaming_job.py` | Đọc Kafka → parse JSON → bỏ message lỗi → `foreachBatch`: hồ sơ + model → `baseline`, `residual` → ngưỡng riêng → đọc lại 2 giờ trước từ Gold → `is_anomaly` → append Gold (idempotent). Log + CSV mỗi batch. |
| `mock_inputs.py` | Tạo Silver giả + model, hồ sơ, ngưỡng **bằng chính code của Task 2**, nên mock có đúng dạng như thật. |

SparkSession dùng chung ở `common/spark_session.py` (có sẵn các xử lý cho Windows và mạng VPN).

## Cài đặt

```bash
make setup     # từ gốc repo: môi trường bdg2 (Python 3.11, Java 17) + extras spark, streaming
```

**Windows:** Spark cần `winutils.exe` + `hadoop.dll` (bản Hadoop 3.4, ví dụ từ `kontext-tech/winutils`)
đặt trong `C:\hadoop\bin`, rồi `setx HADOOP_HOME C:\hadoop` và thêm `%HADOOP_HOME%\bin` vào `PATH`.
Thiếu thì script dừng ngay với thông báo rõ ràng.

## Chạy

Mọi lệnh chạy từ **gốc repo**, Kafka phải đang chạy (`make infra-up`, compose ở `infra/docker-compose.yml`).

**Với dữ liệu thật** (cần `make task1` và `ml_model/train_baseline.py --save` + `ml_model/evaluate.py --save meter_p99_3h`):

```bash
python streaming/streaming_job.py                 # terminal 1, chờ dòng "streaming energy-readings ..."
python streaming/kafka_producer.py --hours 48     # terminal 2, 48 giờ của 2017 = 48 tick x 3s
```

**Với dữ liệu giả:**

```bash
python streaming/mock_inputs.py                                     # 1 lần, ~1 phút
python streaming/streaming_job.py --model-dir streaming/mock_data
python streaming/kafka_producer.py --silver streaming/mock_data/clean_data --hours 30
```

Tùy chọn hay dùng (xem `--help`):

| Lệnh | Tùy chọn |
|---|---|
| `kafka_producer.py` | `--hours N` (mặc định cả năm 2017), `--start`, `--interval`, `--sites A B`, `--spike-count 2`, `--spike-after 10`, `--spike-factor 3`, `--verbose` |
| `streaming_job.py` | `--model-dir` (thư mục chứa đủ 3 đầu ra Task 2), hoặc `--model` / `--profile` / `--thresholds` riêng; `--trigger 5`, `--compact-every 60` |

Lối tắt cho 2 lệnh dữ liệu thật ở trên: `make stream` và `make produce HOURS=48`.

**Chạy lại từ đầu** (Kafka không có volume, nên checkpoint và Gold phải xóa cùng lúc):

```bash
make reset    # infra-down + rm -rf data/gold data/checkpoints + infra-up
```

## Quy ước xử lý

- **Chỉ đồng hồ điện** (`ANOMALY_METER_TYPES`): các loại khác sai số ~100% nên không phát hiện bất thường được (số liệu ở `ml_model/README.md`). Producer chỉ gửi điện; job cũng lọc lại cho chắc.
- **Hồ sơ đồng hồ**: gắn bằng `ml_model.meter_profile.add_profile_features()` trước khi gọi model. Đồng hồ không có lịch sử 2016 không có baseline và bị bỏ qua (đếm ở cột `rows` nhưng không ở `scored`).
- **`baseline = max(prediction, 0)`**: regression có thể dự đoán số kWh âm. Model Task 2 (kể cả train trên `log1p`) đã trả `prediction` đúng đơn vị `meter_reading`, job không cần biến đổi gì.
- **`is_anomaly`**: vượt **ngưỡng riêng của đồng hồ** liên tục **3 giờ** (`ANOMALY_PERSISTENCE_HOURS`). Giờ trước đó được **đọc lại từ chính bảng Gold**, nên restart job vẫn nhớ đúng mà không cần state store riêng. Thiếu 1 giờ thì chuỗi bị ngắt. Hệ quả cho demo: đột biến chỉ được cảnh báo từ giờ thứ 3 của nó.
- **Message lỗi** (không phải JSON, thiếu field, sai kiểu): không làm job crash, bị bỏ qua và được đếm ở cột `invalid` trong log/CSV.
- **Exactly-once**: checkpoint + ghi Delta idempotent (`txnAppId` + `txnVersion` = batch id). Batch bị chạy lại sau restart không ghi trùng.
- **Live demo spike** (khác injected-anomaly offline của Task 2): `--spike-count` đồng hồ tiêu thụ lớn nhất nhân `meter_reading` với `--spike-factor` từ tick `--spike-after`. Tên các tòa nhà này được in ra khi chạy.
- **File nhỏ**: mỗi `--compact-every` batch job chạy `OPTIMIZE` để dashboard đọc nhanh.

## Đọc log

```
batch 14: 1391 rows (0 invalid), 1391 scored, 3 anomalies | 3.66s, 380 rows/s | max latency 7.1s
```

`scored` = số dòng có baseline (điện, có hồ sơ). `rows/s` = số dòng / thời gian xử lý batch. `max latency` = từ lúc producer gửi bản ghi cũ nhất của batch đến lúc batch ghi xong vào Gold (gồm cả thời gian chờ trigger).

## Sự cố thường gặp

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| `HADOOP_HOME is not set` | Windows thiếu winutils, xem mục Cài đặt |
| `Kafka not reachable` / `Topic ... does not exist` | Chưa `make infra-up`; topic do service khởi tạo trong compose tạo |
| `Model not found` / `Meter profile not found` | Chưa train Task 2 với `--save`, hoặc dùng `--model-dir streaming/mock_data` |
| Job lỗi offset sau khi `make infra-down/up` | Kafka đã mới nhưng checkpoint còn cũ → `make reset` |
| Lần chạy đầu tiên chậm | Spark tải jar Delta + Kafka (cần mạng, chỉ 1 lần). Windows: lưu ở `~/.ivy2-bdg2/<hash>` |
| `ClassNotFoundException` / lỗi Jackson sau khi đổi version pyspark/delta-spark | Windows: xóa `~/.ivy2-bdg2` để tải lại jar đúng version |
| `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` lúc tắt | Lỗi vô hại trên Windows (file bị khóa), bỏ qua |
| Dashboard thấy dữ liệu cũ | `data/gold` của lần chạy trước còn đó → `make reset` |

## Kiểm thử

```bash
pytest streaming -m "not integration"   # 41 test, không cần Kafka (~70 s)
pytest streaming                        # + 14 test end-to-end, cần Kafka và mock_data
```

Sau khi tích hợp với `main`: 41 test không cần Kafka và **14 test end-to-end đều đạt** (Kafka 4.2 KRaft
chạy trực tiếp trên máy, cùng cấu hình `infra/docker-compose.yml`). Test end-to-end đã bắt được 1 lỗi
của `monitor()`: báo cáo "idle" của Spark mang `batchId` của batch kế tiếp, làm batch thật bị bỏ khỏi
log/CSV và không chạy compaction; đã sửa. `TEST_REPORT.md` là báo cáo của phiên bản trước khi tích hợp.

## Số liệu quy mô

Chạy thật toàn bộ: Silver đầy đủ → `kafka_producer.py --hours 48` → Kafka → `streaming_job.py` với model,
hồ sơ, ngưỡng thật → Gold → dashboard.

| | Mock (12 tòa nhà, 3 site) | Dữ liệu thật, 48 giờ đầu 2017 qua Kafka |
|---|---|---|
| Bản ghi / tick | 12 | ~1.395 đồng hồ điện, 18 site (66.997 message, 0 lỗi gửi) |
| Thời gian xử lý 1 batch | 2–3 s | trung vị 2,7 s, tối đa 3,3 s (1–2 tick mỗi batch) |
| Độ trễ tối đa (gửi → Gold) | 4–10 s | trung vị 6,1 s, tối đa 7,3 s (gồm thời gian chờ trigger 5 s) |
| Throughput (bản ghi/giây xử lý) | ~4–12 (giới hạn bởi nhịp producer) | trung vị ~950, tối đa ~1.120 |
| Gold | 360/360 | 66.997 dòng, 1.403 đồng hồ, 0 dòng trùng |
| Cảnh báo | — | 2,2% số giờ; 2 tòa nhà bị tạo đột biến (×3 từ tick 10) bị bắt từ giờ thứ 12 (quy tắc 3 giờ) đến hết |

Tỉ lệ cảnh báo theo giờ có đỉnh ~9% vào chiều 2/1/2017 (UTC): thứ Hai nghỉ bù Tết ở Mỹ, model dự đoán
mức của ngày làm việc nên nhiều tòa nhà thấp hơn hẳn baseline.

Cấu hình Spark: `local[*]`, `spark.driver.memory=6g`, `spark.sql.shuffle.partitions=8`, `spark.databricks.delta.snapshotPartitions=4`, trigger 5s. Đo trên Linux, 32 core, Java 17, pyspark 4.2.0, delta-spark 4.4.0.
