# ml_model — Task 2 (Mai)

Train model baseline (mức tiêu thụ bình thường) bằng **Spark MLlib Pipeline** trên toàn bộ tòa nhà.

| | Đường dẫn (`common/config.py`) |
|---|---|
| Input | `CLEAN_DATA_PATH` = `data/silver/clean_data` (Silver) |
| Output | `MODEL_PATH` = `data/models/baseline_model/`, load bằng `PipelineModel.load(MODEL_PATH)` |

- Chia train/test theo thời gian: `timestamp < TEST_START` là train (2016), còn lại là test (2017).
- Pipeline: `StringIndexer(meter_type, primary_use, site_id)` → `VectorAssembler` → Regressor tốt nhất (LR / RF / GBT, chọn theo RMSE).
- Input của model: các cột schema Silver. Output: cột `prediction`.
- Target: TODO (`meter_reading` hay `log1p(meter_reading)`; nếu là `log1p` thì Task 3 phải `expm1(prediction)`).

## Cách chạy

```bash
# từ gốc repo, sau `make setup`
python ml_model/train_baseline.py                   # mẫu 1%, không lưu model
python ml_model/train_baseline.py --sample 1 --save  # toàn bộ dữ liệu, lưu vào MODEL_PATH
python ml_model/evaluate.py         # RMSE/MAE + injected anomaly test
```

## Trạng thái

Bản tối thiểu (`LinearRegression`, mẫu 1%) đã chạy end-to-end trên Silver: 230.205 dòng train, 228.509 dòng test, RMSE 109.287 so với 109.806 của baseline trung bình. Gần như không hơn baseline vì các đồng hồ chênh nhau hàng nghìn lần về độ lớn. Việc tiếp theo: thử target `log1p(meter_reading)`, Random Forest, GBT. `solar` được loại khỏi dữ liệu train.

## Kết quả

| Model | RMSE | MAE | Thời gian train |
|---|---|---|---|
| Trung bình cộng | TODO | TODO | — |
| Linear Regression | TODO | TODO | TODO |
| Random Forest | TODO | TODO | TODO |
| GBT | TODO | TODO | TODO |

Số dòng train / test: TODO. Cấu hình Spark đã dùng: TODO
