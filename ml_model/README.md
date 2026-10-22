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
# từ gốc repo, sau `make setup` và `source .venv/bin/activate`
python ml_model/train_baseline.py                    # mẫu 1%, so sánh lr/rf/gbt x raw/log1p, không lưu (~1-2 phút)
python ml_model/train_baseline.py --sample 0.05      # mẫu 5% (~4 phút)
python ml_model/train_baseline.py --no-profile       # so sánh: không dùng hồ sơ đồng hồ
python ml_model/train_baseline.py --meter-types electricity chilledwater   # thử loại đồng hồ khác

# Model cuối: 100% tập train, lưu model tốt nhất (theo RMSLE) + bảng hồ sơ
SPARK_DRIVER_MEMORY=6g python ml_model/train_baseline.py --models rf --targets raw --sample 1 --save
```

Mặc định chỉ train trên `ANOMALY_METER_TYPES` (`common/config.py`, hiện là `electricity`).

## Bàn giao cho Task 3

| Thành phần | Đường dẫn (`common/config.py`) | Cách dùng |
|---|---|---|
| Model | `MODEL_PATH` = `data/models/baseline_model/` | `PipelineModel.load(MODEL_PATH)`, cột ra `prediction` (đơn vị `meter_reading`, kể cả khi train bằng `log1p`) |
| Hồ sơ đồng hồ | `METER_PROFILE_PATH` = `data/models/meter_profile/` | Bảng Delta; gọi `add_profile_features(df, profile)` (`ml_model/meter_profile.py`) **trước** `model.transform` |
| Loại đồng hồ | `ANOMALY_METER_TYPES` | Chỉ chấm điểm các loại này; đồng hồ không có trong hồ sơ bị loại (không có lịch sử 2016) |

## Quá trình và kết quả

Mọi chỉ số tính trên thang gốc `meter_reading`, tập test là năm 2017. `flagged` là tỉ lệ dòng test bị quy tắc Gold `abs(residual) > baseline * 0.28` gắn cờ; coi năm 2017 là bình thường thì đó là **tỉ lệ báo động giả**, chỉ số quan trọng nhất cho Task 3. Model được chọn theo **RMSLE** (sai số tương đối), không theo RMSE (bị vài đồng hồ khổng lồ chi phối) hay `flagged` (có thể "lách" bằng cách đoán cao lên).

**Lần 1: 7 loại đồng hồ, chưa có hồ sơ** (mẫu 5%, 1.147.213 dòng train): báo động giả tốt nhất **77,7%** (GBT/log1p, RMSLE 1,766). Model không biết mỗi đồng hồ "bình thường dùng bao nhiêu".

**Lần 2: thêm hồ sơ đồng hồ** (trung bình log theo đồng hồ, theo giờ × ngày thường/cuối tuần, tính từ 2016): RMSLE 1,766 → 1,485, báo động giả 77,7% → 55,8%. Phân tích theo loại đồng hồ (chỉ dùng hồ sơ, không model):

| Loại | Báo động giả | Sai số tương đối (trung vị) |
|---|---|---|
| electricity | 29,1% | 15,5% |
| chilledwater, steam | 77–82% | 76–94% |
| hotwater, gas, water | 80–86% | ~100% |
| irrigation | 64% | ~100% |

Các loại ngoài điện thất thường, phụ thuộc mùa, sai số ~100%, nên ngưỡng 28% không áp dụng được → **chỉ phát hiện bất thường cho điện** (`ANOMALY_METER_TYPES`); Silver vẫn giữ đủ 8 loại.

**Lần 3: chỉ điện + hồ sơ theo mùa** (thêm mức theo tháng; mẫu 5%, 577.061 dòng train, 564.638 dòng test):

| Model | Target | RMSE | MAE | RMSLE | flagged | Train (s) |
|---|---|---|---|---|---|---|
| Trung bình cộng | raw | 260,93 | 146,48 | 1,692 | 84,8% | — |
| Chỉ hồ sơ (không model) | — | 90,22 | 28,76 | 0,574 | **26,9%** | — |
| Linear Regression | raw | 179,33 | 100,13 | 1,404 | 76,7% | 5 |
| Random Forest | raw | 84,33 | 29,73 | **0,483** | 28,6% | 29 |
| GBT | raw | **83,32** | **29,47** | 0,533 | 30,0% | 70 |
| Linear Regression | log1p | 89,91 | 28,72 | 0,573 | 26,9% | 2 |
| Random Forest | log1p | 90,27 | 30,93 | 0,536 | 28,1% | 31 |
| GBT | log1p | 88,72 | 31,37 | 0,571 | 28,4% | 70 |

Tham số: RF `numTrees=50, maxDepth=10`; GBT `maxIter=50, maxDepth=6`; `maxMemoryInMB=64` (32 core chạy song song dùng chung bộ nhớ driver; 128 MB đã tràn bộ nhớ 4 GB); LR dùng one-hot. Cấu hình: `SPARK_DRIVER_MEMORY=6g`, `local[*]` 32 core.

**Nhận xét:** báo động giả giảm từ 77,7% xuống ~27–29%. Các model chỉ nhỉnh hơn "chỉ hồ sơ" về RMSLE, gần như bằng về báo động giả: ~27% số giờ bình thường vẫn lệch quá 28% so với thói quen của đồng hồ. Muốn giảm tiếp cần (a) feature "gần đây" (vd. cùng giờ hôm qua — Task 3 phải giữ trạng thái) hoặc (b) ngưỡng riêng theo từng đồng hồ thay cho 28% cố định (cần cả nhóm chốt). Bước 6 (`evaluate.py`, injected anomaly test) sẽ cho số liệu để quyết định.

## Model cuối (100% tập train)

TODO: chạy lệnh "Model cuối" ở trên, ghi RMSE / MAE / RMSLE / flagged / thời gian train.
