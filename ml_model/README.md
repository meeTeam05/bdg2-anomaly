# ml_model — Task 2 (Mai)

Train model baseline (mức tiêu thụ bình thường) bằng **Spark MLlib Pipeline** trên toàn bộ tòa nhà.

| | Đường dẫn (`config.py`) |
|---|---|
| Input | `CLEAN_DATA_PATH` = `data_pipeline/output/clean_data` (Silver) |
| Output | `MODEL_PATH` = `ml_model/models/baseline_model/`, load bằng `PipelineModel.load(MODEL_PATH)` |

- Chia train/test theo thời gian: `timestamp < TEST_START` là train (2016), còn lại là test (2017).
- Pipeline: `StringIndexer(primary_use, site_id)` → `VectorAssembler` → Regressor tốt nhất (LR / RF / GBT, chọn theo RMSE).
- Input của model: các cột schema Silver. Output: cột `prediction`.
- Target: TODO (`meter_reading` hay `log1p(meter_reading)`; nếu là `log1p` thì Task 3 phải `expm1(prediction)`).

## Cách chạy

```bash
pip install -r requirements.txt
python train_baseline.py   # train + lưu model
python evaluate.py         # RMSE/MAE + injected anomaly test
```

## Kết quả

| Model | RMSE | MAE | Thời gian train |
|---|---|---|---|
| Trung bình cộng | TODO | TODO | — |
| Linear Regression | TODO | TODO | TODO |
| Random Forest | TODO | TODO | TODO |
| GBT | TODO | TODO | TODO |

Số dòng train / test: TODO. Cấu hình Spark đã dùng: TODO
