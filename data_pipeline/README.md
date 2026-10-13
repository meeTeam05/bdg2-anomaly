# data_pipeline — Task 1 (Hưng)

CSV gốc BDG2 → **Bronze** → **Silver** (Delta Lake), xử lý **toàn bộ** tòa nhà bằng Spark.

| | Đường dẫn (`config.py`) |
|---|---|
| Input | `RAW_DATA_DIR` = `data_pipeline/raw/` (`electricity.csv`, `weather.csv`, `metadata.csv`) |
| Output Bronze | `BRONZE_DIR` = `data_pipeline/output/bronze/{electricity,weather,metadata}` |
| Output Silver | `CLEAN_DATA_PATH` = `data_pipeline/output/clean_data`, partition theo `site_id`, schema `CLEAN_DATA_SCHEMA` |

## Cách chạy

```bash
pip install -r requirements.txt
python ingest.py       # CSV -> Bronze
python preprocess.py   # Bronze -> Silver
```

## Số liệu quy mô

| Tầng | Số dòng | Số tòa nhà | Dung lượng | Thời gian chạy |
|---|---|---|---|---|
| Bronze | TODO | TODO | TODO | TODO |
| Silver | TODO | TODO | TODO | TODO |

Cấu hình Spark đã dùng: TODO
