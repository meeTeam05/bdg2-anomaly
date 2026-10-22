# ml_model — Task 2 (Mai)

Train model baseline (mức tiêu thụ bình thường) bằng **Spark MLlib Pipeline** trên toàn bộ tòa nhà.

| | Đường dẫn (`common/config.py`) |
|---|---|
| Input | `CLEAN_DATA_PATH` = `data/silver/clean_data` (Silver) |
| Output | `MODEL_PATH` = `data/models/baseline_model/`, load bằng `PipelineModel.load(MODEL_PATH)` |

- Chia train/test theo thời gian: `timestamp < TEST_START` là train (2016), còn lại là test (2017).
- Pipeline: `StringIndexer(meter_type, primary_use, site_id)` → `VectorAssembler` → Regressor tốt nhất (LR / RF / GBT, chọn theo RMSE).
- Input của model: các cột schema Silver. Output: cột `prediction`.
- Target: `meter_reading` (`raw`) hoặc `log1p(meter_reading)` (`log1p`). Với `log1p`, pipeline đã có sẵn bước `expm1` ở cuối nên cột `prediction` luôn ở đơn vị `meter_reading`, **Task 3 không cần biến đổi gì thêm**.

## Cách chạy

```bash
# từ gốc repo, sau `make setup`
python ml_model/train_baseline.py                   # mẫu 1%, so sánh lr/rf/gbt x raw/log1p, không lưu
python ml_model/train_baseline.py --sample 0.05      # mẫu 5%
python ml_model/train_baseline.py --models gbt --targets log1p --sample 1 --save
                                                     # toàn bộ tập train, lưu model tốt nhất (RMSE) vào MODEL_PATH
python ml_model/evaluate.py         # RMSE/MAE + injected anomaly test
```

## Trạng thái

Bước 1–2 (chọn target, so sánh model) đã chạy trên mẫu 5% (`SPARK_DRIVER_MEMORY=6g`, `local[*]` 32 core): 1.147.213 dòng train (2016), 1.143.654 dòng test (2017), `solar` đã loại. Mọi chỉ số tính trên thang gốc `meter_reading`. `flagged` là tỉ lệ dòng test bị quy tắc Gold `abs(residual) > baseline * 0.28` gắn cờ, xấp xỉ tỉ lệ báo động giả nếu coi năm 2017 là bình thường.

| Model | Target | RMSE | MAE | RMSLE | flagged | Train (s) |
|---|---|---|---|---|---|---|
| Trung bình cộng | raw | 111.630 | 23.574 | 5,926 | 99,3% | — |
| Linear Regression | raw | 105.058 | 31.042 | 5,109 | 98,8% | 4 |
| Random Forest | raw | **58.586** | **7.953** | 2,928 | 83,5% | 16 |
| GBT | raw | 64.090 | 8.391 | 3,607 | 91,9% | 37 |
| Linear Regression | log1p | 111.741 | 12.096 | 2,321 | 87,1% | 2 |
| Random Forest | log1p | 99.031 | 10.415 | 1,829 | 80,2% | 16 |
| GBT | log1p | 82.481 | 9.261 | **1,766** | **77,7%** | 37 |

Tham số: RF `numTrees=50, maxDepth=10`; GBT `maxIter=50, maxDepth=6`; LR dùng one-hot cho biến phân loại.

## Kết quả

| Model | RMSE | MAE | Thời gian train |
|---|---|---|---|
| Trung bình cộng | TODO | TODO | — |
| Linear Regression | TODO | TODO | TODO |
| Random Forest | TODO | TODO | TODO |
| GBT | TODO | TODO | TODO |

Số dòng train / test: TODO. Cấu hình Spark đã dùng: TODO
