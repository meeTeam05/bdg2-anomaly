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
SPARK_DRIVER_MEMORY=10g python ml_model/train_baseline.py --models rf --targets raw --sample 1 --save

# Bước 6: injected anomaly test (cần model đã lưu ở trên, ~45 giây)
SPARK_DRIVER_MEMORY=10g python ml_model/evaluate.py                       # so 6 quy tắc cảnh báo
SPARK_DRIVER_MEMORY=10g python ml_model/evaluate.py --save meter_p99_3h   # lưu ngưỡng của quy tắc đã chọn
```

Mặc định chỉ train trên `ANOMALY_METER_TYPES` (`common/config.py`, hiện là `electricity`).

## Bàn giao cho Task 3

| Thành phần | Đường dẫn (`common/config.py`) | Cách dùng |
|---|---|---|
| Model | `MODEL_PATH` = `data/models/baseline_model/` | `PipelineModel.load(MODEL_PATH)`, cột ra `prediction` (đơn vị `meter_reading`, kể cả khi train bằng `log1p`) |
| Hồ sơ đồng hồ | `METER_PROFILE_PATH` = `data/models/meter_profile/` | Bảng Delta; gọi `add_profile_features(df, profile)` (`ml_model/meter_profile.py`) **trước** `model.transform` |
| Loại đồng hồ | `ANOMALY_METER_TYPES` | Chỉ chấm điểm các loại này; đồng hồ không có trong hồ sơ bị loại (không có lịch sử 2016) |
| Ngưỡng bất thường | `METER_THRESHOLDS_PATH` = `data/models/meter_thresholds/` | Bảng Delta (`building_id`, `meter_type`, `anomaly_threshold`); `is_anomaly` khi `abs(residual) > baseline * anomaly_threshold` liên tục `ANOMALY_PERSISTENCE_HOURS` (3) giờ; thiếu ngưỡng thì dùng `THRESHOLD_RATIO` |

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

Lệnh: `SPARK_DRIVER_MEMORY=10g python ml_model/train_baseline.py --models rf --targets raw --sample 1 --save` — toàn bộ dữ liệu điện, tổng 155 s, train RF 106 s (32 core).

| Model | RMSE | MAE | RMSLE | flagged | Train (s) |
|---|---|---|---|---|---|
| Trung bình cộng | 261,52 | 146,46 | 1,694 | 84,7% | — |
| Chỉ hồ sơ (không model) | 90,01 | 28,54 | 0,569 | **26,8%** | — |
| **Random Forest, raw (đã lưu)** | **81,61** | 28,78 | **0,483** | 28,6% | 106 |

- Đã lưu: `data/models/baseline_model/` (StringIndexer ×3 → VectorAssembler → RandomForestRegressionModel, 4,9 MB) và `data/models/meter_profile/` (813.888 dòng = 1.413 đồng hồ × 12 tháng × 24 giờ × 2 loại ngày, 11 MB). Đã kiểm tra `PipelineModel.load` + `add_profile_features` + `transform` ra cột `prediction`.
- RF 50 cây, `maxDepth=10`. Chạy 100% cần `SPARK_DRIVER_MEMORY=10g`: RF giữ 1 trọng số cho mỗi cây trên mỗi dòng (11,5 triệu dòng train), 6 GB bị tràn bộ nhớ.
- **Train trên 100% không tốt hơn mẫu 5%** (RMSLE 0,483 cả hai): giới hạn nằm ở feature, không phải lượng dữ liệu.
- RF tốt hơn "chỉ hồ sơ" về sai số (RMSLE 0,483 so với 0,569) nhưng **báo động giả gần bằng** (28,6% so với 26,8%). Bước 6 (`evaluate.py`) sẽ đo khả năng bắt bất thường thật và đề xuất ngưỡng.

## Bước 6: Injected anomaly test (`evaluate.py`)

Câu hỏi: quy tắc cảnh báo có **bắt được bất thường thật** mà **không báo nhầm quá nhiều** không? Giảm báo nhầm thì dễ (nâng ngưỡng lên) nhưng sẽ bỏ sót, nên phải đo cả hai.

- Dự đoán toàn bộ điện năm 2017 bằng model đã lưu. **Quý 1** (2.815.847 dòng) để hiệu chỉnh: với mỗi đồng hồ, đo độ lệch bình thường `|thực tế − baseline| / baseline`, lấy phân vị 95% / 99% làm **ngưỡng riêng**. **Tháng 4–12** (8.475.735 dòng) để kiểm tra.
- Tiêm vào mỗi đồng hồ **1 khoảng 24 giờ** bất thường giả: ×1,5 (lãng phí), ×2 (lãng phí nặng), ×0,5 (thiết bị tắt/hỏng). "Bắt được" = có ít nhất 1 giờ trong khoảng đó bị cảnh báo; "báo nhầm" = % giờ bình thường bị cảnh báo.
- Tiêu chí chọn (đặt trước khi chạy): bắt được nhiều ×1,5 nhất với báo nhầm ≤ 5%.

| Quy tắc | Báo nhầm | Bắt ×1,5 | Bắt ×2 | Bắt ×0,5 |
|---|---|---|---|---|
| Cố định 28%, 1 giờ (thiết kế ban đầu) | 29,0% | 96,6% | 99,2% | 99,7% |
| Cố định 28%, 3 giờ liền | 21,8% | 90,6% | 98,2% | 98,9% |
| Ngưỡng riêng p95, 1 giờ | 11,9% | 80,7% | 91,7% | 79,6% |
| Ngưỡng riêng p95, 3 giờ liền | 8,0% | 71,6% | 87,6% | 75,9% |
| Ngưỡng riêng p99, 1 giờ | 5,6% | 71,6% | 87,4% | 63,1% |
| **Ngưỡng riêng p99, 3 giờ liền (đã chọn)** | **4,0%** | 59,4% | 81,9% | 58,8% |

**Kết luận:** quy tắc cũ bắt gần hết nhưng báo nhầm 29% số giờ bình thường, nên trên dashboard gần như tòa nhà nào cũng luôn có cảnh báo. Quy tắc mới giảm báo nhầm **khoảng 7 lần (29% → 4%)**, vẫn bắt 82% trường hợp lãng phí gấp đôi; đổi lại bỏ sót nhiều hơn với bất thường nhẹ (×1,5 bắt 59%). Nếu nhóm ưu tiên bắt nhiều hơn, `meter_p95_3h` (báo nhầm 8%, bắt ×1,5 72%) là lựa chọn kế tiếp.

**Giới hạn:** ngưỡng trung vị 54%, 90% đồng hồ ≤ 148%, nhưng vài đồng hồ có mức dùng rất nhỏ hoặc rất thất thường có ngưỡng rất lớn (tối đa khoảng 7.400%) nên gần như không bao giờ cảnh báo. Quý 1/2017 dùng để hiệu chỉnh cũng nằm trong giai đoạn Task 3 replay.
