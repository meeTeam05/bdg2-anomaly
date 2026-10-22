# data_pipeline — Task 1 (Hưng)

CSV gốc BDG2 → **Bronze** → **Silver** (Delta Lake), xử lý **toàn bộ** tòa nhà bằng Spark.

| | Đường dẫn (`common/config.py`) |
|---|---|
| Input | `RAW_DATA_DIR` = `data/raw/` (tải bằng `make data`) |
| Output Bronze | `BRONZE_DIR` = `data/bronze/<tên file>` |
| Output Silver | `CLEAN_DATA_PATH` = `data/silver/clean_data`, partition theo `site_id`, schema `CLEAN_DATA_SCHEMA` |

## Các bước

| Bước | File | Việc làm |
|---|---|---|
| 1. Ingestion | `ingest.py` | Đọc 10 CSV, ghi **nguyên trạng** ra 10 bảng Delta Bronze (thêm 2 cột nguồn gốc `_source_file`, `_ingested_at`) |
| 2. EDA | `notebooks/eda.ipynb` | Khám phá Bronze bằng Spark → danh sách quyết định làm sạch (mục 8 của notebook) |
| 3. Preprocess | `preprocess.py` | Unpivot, làm sạch, join, tạo feature, chuyển UTC, ghi Silver theo các quyết định D1–D11 ở bước 2 |
| 4. Validate | `validate_silver.py` | Kiểm tra Silver theo hợp đồng dữ liệu (11 kiểm tra), mã lỗi ≠ 0 nếu có kiểm tra không đạt |

## Cách chạy

Chạy cả Task 1 bằng 1 lệnh (từ gốc repo, sau `make setup` và `make data`):

```bash
make task1    # ingest -> preprocess -> validate, dừng ngay nếu 1 bước lỗi (~3-4 phút)
```

Hoặc từng bước:

```bash
# từ gốc repo, sau `make setup`, `make data` và `source .venv/bin/activate`
python data_pipeline/ingest.py                    # CSV -> Bronze (cả 10 nguồn)
python data_pipeline/ingest.py weather metadata   # chỉ ingest lại vài nguồn
python data_pipeline/preprocess.py                # Bronze -> Silver (~2,5 phút)
python data_pipeline/validate_silver.py           # kiểm tra Silver trước khi bàn giao
pytest data_pipeline                              # 14 test cho các hàm làm sạch
```

Bronze giữ nguyên dữ liệu gốc: không đổi tên cột, không bỏ dòng, giá trị thiếu vẫn để null. Timestamp trong BDG2 không có múi giờ; Bronze lưu đúng giá trị đồng hồ đó (session Spark ở UTC), việc xác định múi giờ thật làm ở Silver.

## Số liệu quy mô

**Bronze** (`ingest.py`, 10 nguồn, tổng ~25 giây):

| Nguồn | Dòng | Cột (không tính cột nguồn gốc) | CSV | Delta |
|---|---|---|---|---|
| electricity | 17.544 | 1.579 (timestamp + 1.578 tòa nhà) | 167 MB | 109 MB |
| chilledwater | 17.544 | 556 | 76 MB | 52 MB |
| steam | 17.544 | 371 | 45 MB | 21 MB |
| hotwater | 17.544 | 186 | 24 MB | 9,3 MB |
| gas | 17.544 | 178 | 20 MB | 4,1 MB |
| water | 17.544 | 147 | 15 MB | 2,2 MB |
| irrigation | 17.544 | 38 | 3,1 MB | 0,4 MB |
| solar | 17.544 | 6 | 0,7 MB | 0,2 MB |
| weather | 331.166 | 10 | 19 MB | 2,8 MB |
| metadata | 1.636 | 32 | 0,3 MB | 0,1 MB |
| **Tổng** | **3.053 đồng hồ × 17.544 giờ = 53.561.832 số đo** | | **367 MB** | **200 MB** |

**Silver** (`preprocess.py`, ~2,5 phút, 441 MB, partition theo `site_id`):

| Bước (quyết định trong EDA) | Số dòng còn lại | Bị loại |
|---|---|---|
| D1 xoay 8 bảng đồng hồ sang dạng long | 53.561.832 | |
| D2 bỏ null / số âm | 50.302.251 | 3.259.581 |
| D3+D4 bỏ flatline ≥ 24h | 47.035.309 | 3.266.942 |
| D5 bỏ outlier > 10 × p99 | 47.034.590 | 719 |
| D6 bỏ đồng hồ còn < 50% số giờ | 46.037.047 | 997.543 |
| D7+D8 join thời tiết, chuyển UTC, bỏ giờ DST trùng | **45.900.387** | 136.660 |

| meter_type | Đồng hồ | Dòng | Train (2016) | Test (2017) |
|---|---|---|---|---|
| electricity | 1.413 | 22.845.190 | 11.553.608 | 11.291.582 |
| chilledwater | 519 | 8.755.563 | 4.320.988 | 4.434.575 |
| steam | 335 | 5.629.931 | 2.779.528 | 2.850.403 |
| gas | 169 | 2.841.785 | 1.460.979 | 1.380.806 |
| hotwater | 165 | 2.779.853 | 1.397.753 | 1.382.100 |
| water | 142 | 2.406.025 | 1.179.805 | 1.226.220 |
| irrigation | 36 | 572.106 | 284.575 | 287.531 |
| solar | 4 | 69.934 | 34.967 | 34.967 |

Tổng: 1.491 tòa nhà, 18 site, 16 loại hình. `validate_silver.py`: 11/11 kiểm tra đạt.

**Site Swan bị loại hoàn toàn** (19 → 18 site): Swan chỉ có dữ liệu năm 2017, nên mọi đồng hồ của Swan còn dưới 50% số giờ (D6). Loại là hợp lý vì model train trên 2016, không có gì để học cho Swan. Swan cũng là site duy nhất thiếu `primary_use`, nên Silver không còn nhãn `"Unknown"` (D10 vẫn giữ trong code để phòng dữ liệu mới).

Cấu hình: Spark 4.2.0 `local[*]` (32 core), `SPARK_DRIVER_MEMORY=6g`, Python 3.11, Java 17.
