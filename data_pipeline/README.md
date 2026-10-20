# data_pipeline — Task 1 (Hưng)

CSV gốc BDG2 → **Bronze** → **Silver** (Delta Lake), xử lý **toàn bộ** tòa nhà bằng Spark.

| | Đường dẫn (`common/config.py`) |
|---|---|
| Input | `RAW_DATA_DIR` = `data/raw/` (tải bằng `make data`) |
| Output Bronze | `BRONZE_DIR` = `data/bronze/<tên file>` |
| Output Silver | `CLEAN_DATA_PATH` = `data/silver/clean_data`, partition theo `site_id`, schema `CLEAN_DATA_SCHEMA` |

EDA nằm ở `notebooks/eda.ipynb`.

## Cách chạy

```bash
# từ gốc repo, sau `make setup` và `make data`
python data_pipeline/ingest.py       # CSV -> Bronze
python data_pipeline/preprocess.py   # Bronze -> Silver
```

## Số liệu quy mô

| Tầng | Số dòng | Số tòa nhà | Dung lượng | Thời gian chạy |
|---|---|---|---|---|
| Bronze | TODO | TODO | TODO | TODO |
| Silver | TODO | TODO | TODO | TODO |

Cấu hình Spark đã dùng: TODO
