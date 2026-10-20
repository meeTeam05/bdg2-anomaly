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
| 3. Preprocess | `preprocess.py` | Unpivot, làm sạch, join, tạo feature, ghi Silver theo các quyết định ở bước 2 |

## Cách chạy

```bash
# từ gốc repo, sau `make setup`, `make data` và `source .venv/bin/activate`
python data_pipeline/ingest.py                    # CSV -> Bronze (cả 10 nguồn)
python data_pipeline/ingest.py weather metadata   # chỉ ingest lại vài nguồn
python data_pipeline/preprocess.py                # Bronze -> Silver
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

**Silver:** TODO sau bước 3.

Cấu hình: Spark 4.2.0 `local[*]` (32 core), `SPARK_DRIVER_MEMORY=6g`, Python 3.11, Java 17.
