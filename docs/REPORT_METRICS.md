# Số liệu Big Data cho báo cáo

Bằng chứng "Big Data" theo yêu cầu ở `docs/TASKS.md`: số dòng từng tầng, dung lượng, partition, thời
gian từng job, cấu hình Spark. Mọi số đo trên dữ liệu BDG2 **đầy đủ**, ngày 22/10/2026, commit `92905e1`.
Cách đo lại ở mục cuối.

## Môi trường đo

| | |
|---|---|
| Máy | 1 máy, 32 core, 16 GB RAM, Ubuntu 24.04 |
| Phần mềm | Java 17.0.20, Python 3.11, pyspark 4.2.0, delta-spark 4.4.0, Kafka 4.2.0 (KRaft, Docker) |
| Spark | `local[*]` (32 luồng), driver memory 6 GB (train model cuối: 10 GB), session timezone UTC |
| Cấu hình Spark batch | `spark.sql.adaptive.advisoryPartitionSizeInBytes=32m`, Delta Lake extensions |
| Cấu hình Spark streaming | `spark.sql.shuffle.partitions=8`, `spark.databricks.delta.snapshotPartitions=4`, trigger 5 s |

Cùng một code chạy được trên cluster: chỉ đổi `master` và đường dẫn `data/` sang HDFS/S3
(`common/spark_session.py`, `BDG2_DATA_DIR`).

## 1. Tổng quan theo tầng (Medallion)

| Tầng | Bảng | Số dòng | Dung lượng | File | Partition |
|---|---|---|---|---|---|
| Raw | 10 file CSV | 53.561.832 số đo (17.544 giờ × 3.053 cột đồng hồ) + 331.166 dòng thời tiết + 1.636 tòa nhà | 384 MB | 10 | — |
| Bronze | 10 bảng Delta (giữ nguyên dạng CSV) | 473.154 dòng "rộng" (= 53,56 triệu số đo) | 206 MB | 86 | không |
| Silver | `clean_data` | **45.900.387** | 520 MB | 1.800 | 18 (`site_id`) |
| Model | `baseline_model` + `meter_profile` + `meter_thresholds` | 813.888 dòng hồ sơ, 1.412 ngưỡng | 15,1 MB | — | — |
| Gold | `energy_results` (replay 48 giờ đầu 2017) | 66.997 | — | — | 18 (`site_id`) |
| Gold | ước tính replay cả năm 2017 | ~12,3 triệu (1.403 đồng hồ × 8.760 giờ) | — | — | 18 |

Dung lượng là của phiên bản hiện tại (tổng `size_bytes` trong Delta log), dạng Parquet nén: Bronze
chỉ bằng ~54% CSV gốc. Thư mục trên ổ đĩa lớn hơn (Silver ~1,4 GB) vì Delta giữ file của các lần ghi
đè trước để time travel; `VACUUM` sẽ xóa chúng.

Silver gồm 8 loại đồng hồ, 2.783 đồng hồ, 18 site, giai đoạn 2016–2017:

| Loại đồng hồ | Số đồng hồ | Số dòng | Train (2016) | Test (2017) |
|---|---|---|---|---|
| electricity | 1.413 | 22.845.190 | 11.553.608 | 11.291.582 |
| chilledwater | 519 | 8.755.563 | 4.320.988 | 4.434.575 |
| steam | 335 | 5.629.931 | 2.779.528 | 2.850.403 |
| gas | 169 | 2.841.785 | 1.460.979 | 1.380.806 |
| hotwater | 165 | 2.779.853 | 1.397.753 | 1.382.100 |
| water | 142 | 2.406.025 | 1.179.805 | 1.226.220 |
| irrigation | 36 | 572.106 | 284.575 | 287.531 |
| solar | 4 | 69.934 | 34.967 | 34.967 |

## 2. Task 1 — Batch: CSV → Bronze → Silver

| Job | Thời gian |
|---|---|
| `ingest.py` (10 CSV → Bronze) | ~28 s (riêng electricity, 1.579 cột: 12,5 s) |
| `preprocess.py` (Bronze → Silver, D1–D11) | 144 s |
| `validate_silver.py` | 11/11 kiểm tra đạt |
| **`make task1`** | ~3–4 phút |

Làm sạch từng bước (mỗi dòng là số dòng còn lại):

| Bước | Còn lại | Bị loại |
|---|---|---|
| D1 unpivot từ Bronze | 53.561.832 | |
| D2 bỏ giá trị null / âm | 50.302.251 | 3.259.581 |
| D3+D4 bỏ đoạn đứng yên ≥ 24 giờ | 47.035.309 | 3.266.942 |
| D5 bỏ ngoại lai > 10 × p99 | 47.034.590 | 719 |
| D6 bỏ đồng hồ có < 50% số giờ | 46.037.047 | 997.543 |
| D7+D8 join thời tiết, đổi sang UTC, bỏ trùng giờ DST | **45.900.387** | 136.660 |

Giữ lại 85,7% số đo. Kiểm tra Silver: đúng schema, 0 null, 0 giá trị âm, 0 khóa trùng
(`building_id`, `meter_type`, `timestamp`), giờ/thứ hợp lệ, timestamp UTC, partition theo `site_id`.

## 3. Task 2 — Spark MLlib

| | |
|---|---|
| Phạm vi | Đồng hồ điện, 1.413 đồng hồ |
| Train / test | 11.553.608 dòng (2016) / 11.291.582 dòng (2017), chia theo thời gian |
| Pipeline | StringIndexer ×3 → VectorAssembler → RandomForestRegressor (50 cây, maxDepth 10) |
| Feature | thời gian, thời tiết, tòa nhà, `meter_type` + hồ sơ đồng hồ (813.888 dòng = 1.413 × 12 tháng × 24 giờ × 2 loại ngày) |
| Thời gian | train RF 106 s, cả job 155 s (driver 10 GB) |
| Ngưỡng (`evaluate.py`) | 44 s; 2.815.847 dòng hiệu chỉnh (quý 1/2017), 8.475.735 dòng đánh giá |

| Model | RMSE | MAE | RMSLE |
|---|---|---|---|
| Trung bình cộng | 261,52 | 146,46 | 1,694 |
| Chỉ hồ sơ đồng hồ | 90,01 | 28,54 | 0,569 |
| **Random Forest (đã lưu)** | **81,61** | 28,78 | **0,483** |

Quy tắc cảnh báo, đo bằng injected anomaly test: hiệu chỉnh ngưỡng trên quý 1/2017, kiểm tra trên
tháng 4–12/2017 (mỗi đồng hồ được tiêm 1 khoảng 24 giờ bất thường; "bắt được" = ít nhất 1 giờ trong
khoảng đó bị cảnh báo, "báo nhầm" = % giờ bình thường bị cảnh báo):

| Quy tắc | Báo nhầm | Bắt ×1,5 | Bắt ×2 | Bắt ×0,5 |
|---|---|---|---|---|
| Cố định 28%, 1 giờ (thiết kế ban đầu) | 29,0% | 96,6% | 99,2% | 99,7% |
| **Ngưỡng riêng p99, 3 giờ liền (đã chọn)** | **4,0%** | 59,4% | 81,9% | 58,8% |

Giảm báo nhầm ~7 lần, đổi lại bỏ sót nhiều hơn với bất thường nhẹ (×1,5).
Chi tiết các lần thử ở `ml_model/README.md`.

## 4. Task 3 — Streaming (Velocity)

Replay 48 giờ đầu 2017 trên toàn bộ đồng hồ điện: producer → Kafka → Spark Structured Streaming
(model + hồ sơ + ngưỡng thật) → Gold.

| | |
|---|---|
| Producer | 18 thread (1 thread / site), 1 tick = 1 giờ dữ liệu mỗi 3 s; 66.997 message, 0 lỗi |
| Bản ghi mỗi tick | ~1.395 đồng hồ điện |
| Thời gian xử lý 1 micro-batch | trung vị 2,7 s, tối đa 3,3 s |
| **Throughput** | trung vị ~950 bản ghi/s, tối đa ~1.120 bản ghi/s |
| **Độ trễ** (producer gửi → ghi xong vào Gold) | trung vị 6,1 s, tối đa 7,3 s (gồm thời gian chờ trigger 5 s) |
| Đúng 1 lần (exactly-once) | Gold 66.997 dòng = 66.997 message, 0 dòng trùng; tắt/bật lại job giữa chừng vẫn không mất, không trùng (test end-to-end) |
| Cảnh báo | 2,2% số giờ; 2 tòa nhà bị tạo đột biến ×3 từ tick 10 bị bắt từ giờ thứ 12 (quy tắc 3 giờ liền) đến hết |

Hệ thống chạy dưới khả năng: mỗi batch xử lý ~2,7 s trong khi producer đưa 1 giờ dữ liệu mỗi 3 s và
trigger 5 s. Khả năng mở rộng: tăng số partition Kafka (hiện 1) và chạy Spark trên cluster, không đổi
code.

## 5. Task 4 — Dashboard

| | |
|---|---|
| Quy mô hiển thị | 1.403 đồng hồ, 18 site |
| Lần vẽ đầu | 1,9 s |
| Mỗi lần refresh (5 s) | 0,1 s (chỉ đọc lại khi Delta có version mới) |
| Lượng đọc | tối đa 168 giờ gần nhất (~235 nghìn dòng): Delta bỏ qua file cũ dựa vào thống kê trong log, nên không tăng theo thời gian chạy |

## 6. Hạn chế (nên nêu trong báo cáo)

- Chỉ phát hiện bất thường cho **đồng hồ điện**. Các loại khác sai số tương đối ~100% nên ngưỡng không dùng được.
- Một số đồng hồ dùng rất ít hoặc thất thường có ngưỡng rất lớn (tối đa ~7.400%), gần như không bao giờ cảnh báo.
- Model chưa có thông tin ngày lễ: ngày nghỉ bù Tết 2/1/2017 có ~9% đồng hồ bị cảnh báo trong 1 giờ.
- Streaming mới đo trên 48 giờ replay; chưa chạy hết cả năm 2017 (~7 giờ).
- Ngưỡng được hiệu chỉnh trên quý 1/2017, cũng là giai đoạn đầu của replay (demo 48 giờ nằm trong đó). Số báo nhầm/bắt được ở mục 3 đo trên tháng 4–12, không trùng giai đoạn hiệu chỉnh.
- Gold sinh nhiều file nhỏ (mỗi batch × 18 partition); job tự gộp (`OPTIMIZE`) mỗi 60 batch.

## Cách đo lại

```bash
make task1                 # số dòng từng bước D1–D8 và thời gian Silver in ra màn hình
make task2                 # bảng RMSE/MAE/RMSLE, thời gian train, tỉ lệ bắt bất thường
make demo HOURS=48         # log mỗi batch: rows, rows/s, max latency; lưu ở data/gold/batch_metrics.csv
```

Số dòng / dung lượng / số file của một bảng Delta (không cần Spark):

```bash
python -c "
import pyarrow as pa; from deltalake import DeltaTable
t = DeltaTable('data/silver/clean_data'); a = pa.table(t.get_add_actions(flatten=True))
print(sum(a['num_records'].to_pylist()), 'rows', a.num_rows, 'files', sum(a['size_bytes'].to_pylist()) // 10**6, 'MB')"
```
