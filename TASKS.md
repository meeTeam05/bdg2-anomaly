# bdg2-anomaly — Phân công công việc Giai đoạn 1

**Tên đề tài:** Giám sát và phát hiện bất thường tiêu thụ năng lượng tòa nhà thương mại theo thời gian thực bằng Apache Kafka và Spark từ dữ liệu IoT (Building Data Genome 2)

**Nhóm:** Nguyễn Quốc Hưng (23139019) · Phan Đặng Quỳnh Mai (23139028) · Trần Trọng Nguyên (23139030) · Lê Quang Minh Nhật (23139031)

**Ý tưởng tổng quát:** Hệ thống học một mô hình Regression để biết "mức tiêu thụ năng lượng BÌNH THƯỜNG của 1 tòa nhà nên là bao nhiêu" (baseline), sau đó mô phỏng dữ liệu tòa nhà gửi liên tục qua Kafka, dùng Spark Streaming so sánh giá trị THỰC TẾ với baseline đó — nếu lệch (residual) quá ngưỡng thì cảnh báo bất thường (nghi lãng phí điện / thiết bị hỏng), hiển thị lên dashboard.

---

## 1. Cấu trúc thư mục (mỗi người CHỈ code trong thư mục của mình)

```
bdg2-anomaly/
├── data_pipeline/        ← Hưng
│   ├── eda.ipynb
│   ├── preprocess.py
│   └── README.md
├── ml_model/              ← Mai
│   ├── train_baseline.py
│   ├── evaluate.py
│   ├── models/            (model đã train, .gitignore file lớn)
│   └── README.md
├── streaming/              ← Nguyên
│   ├── kafka_producer.py
│   ├── streaming_job.py
│   └── README.md
├── dashboard/              ← Nhật
│   ├── dashboard.py
│   ├── docker-compose.yml
│   └── README.md
├── docs/                   ← chung (báo cáo, slide, đề xuất đề tài)
└── README.md               ← tổng quan cả repo
```

**Quy tắc:** không sửa file trong thư mục của người khác. Nếu cần đổi (ví dụ đổi tên cột), phải báo trong group chat trước, vì các thư mục phụ thuộc lẫn nhau qua "hợp đồng dữ liệu" ở mục 3.

---

## 2. Task chi tiết theo người

### Task 1 — Data Pipeline (Hưng)
**Input:** File gốc BDG2 (`electricity.csv`, `weather.csv`, `metadata.csv`)
**Việc làm:**
- Tải dữ liệu, đọc bằng Spark, EDA (schema, số dòng, tỉ lệ thiếu dữ liệu)
- Lọc 30–50 tòa nhà cùng loại hình (`primary_use = "Office"`)
- Ghép 3 nguồn theo `building_id` + `timestamp` + `site_id`
- Tạo feature: `hour_of_day`, `day_of_week`, `is_weekend`
- Làm sạch: loại giá trị âm, giá trị đứng yên bất thường (flatline), xử lý null
**Output bắt buộc:** 1 file Parquet/Delta đúng schema ở mục 3, đặt tại `data_pipeline/output/clean_data.parquet`
**Done khi:** Mai chạy được `train_baseline.py` trên file này không lỗi.

### Task 2 — ML Model (Mai)
**Input:** `data_pipeline/output/clean_data.parquet` (từ Task 1)
**Việc làm:**
- Feature pipeline: StringIndexer (`primary_use`), VectorAssembler
- Chia train/test theo thời gian (không random)
- Train + so sánh: Linear Regression, Random Forest, GBT
- Đánh giá RMSE/MAE, so với baseline "trung bình cộng"
- Injected anomaly test (tiêm bất thường giả, kiểm tra ngưỡng residual)
**Output bắt buộc:** model lưu tại `ml_model/models/baseline_rf/`, load được bằng `PipelineModel.load(...)`
**Done khi:** Nguyên load được model này trong `streaming_job.py` và ra được `prediction` không lỗi.

### Task 3 — Streaming Pipeline (Nguyên)
**Input:** `data_pipeline/output/clean_data.parquet` (để replay) + model từ Task 2
**Việc làm:**
- Kafka Producer: multithreading mô phỏng nhiều tòa nhà, mỗi thread gửi dữ liệu 1 tòa nhà đều đặn (2–3s/bản ghi)
- Kịch bản: 1–2 tòa nhà tăng đột biến sau X tick (mô phỏng thiết bị hỏng)
- Spark Structured Streaming: load model Task 2 → tính `baseline`, `residual`, `is_anomaly` (ngưỡng 28%)
- Ghi kết quả vào Delta Lake
**Output bắt buộc:** bảng Delta Lake tại `streaming/output/energy_results` đúng schema mục 3
**Done khi:** Nhật đọc được bảng này và hiển thị đúng lên dashboard.

### Task 4 — Dashboard & Hạ tầng (Nhật)
**Input:** `streaming/output/energy_results` (từ Task 3)
**Việc làm:**
- `docker-compose.yml`: Kafka + Zookeeper + Spark (dùng chung cho cả nhóm)
- Dashboard (Streamlit) đọc Delta Lake, tự refresh
- Hiển thị: danh sách tòa nhà + trạng thái, biểu đồ thực tế vs baseline, log sự kiện
**Output bắt buộc:** `docker compose up` chạy được hạ tầng; `streamlit run dashboard.py` xem được tại `localhost:8501`
**Done khi:** Cả nhóm demo thử end-to-end không lỗi.

---

## 3. Hợp đồng dữ liệu (Data Contract) — BẮT BUỘC tuân thủ để không lệch nhau

Đây là phần quan trọng nhất giúp 4 người làm song song mà không đụng nhau. Tên cột, kiểu dữ liệu dưới đây là **cố định**, ai muốn đổi phải được cả nhóm đồng ý trước.

**Schema đầu ra Task 1 → Task 2, Task 3:**
| Cột | Kiểu | Ghi chú |
|---|---|---|
| `building_id` | string | |
| `timestamp` | timestamp | |
| `meter_reading` | double | kWh |
| `air_temperature` | double | |
| `dew_temperature` | double | liên quan độ ẩm |
| `hour_of_day` | int | 0–23 |
| `day_of_week` | int | 1–7 |
| `is_weekend` | int | 0/1 |
| `square_feet` | double | |
| `primary_use` | string | |

**Schema đầu ra Task 3 → Task 4:**
| Cột | Kiểu | Ghi chú |
|---|---|---|
| `building_id` | string | |
| `timestamp` | timestamp | |
| `meter_reading` | double | giá trị thực tế |
| `baseline` | double | model dự đoán |
| `residual` | double | `meter_reading - baseline` |
| `is_anomaly` | boolean | `abs(residual) > baseline * 0.28` |

**Ngưỡng dùng chung:** `THRESHOLD_RATIO = 0.28` — khai báo 1 chỗ duy nhất (file `config.py` ở gốc repo), Task 2 và Task 3 cùng import từ đó, không hardcode riêng mỗi nơi.

---

## 4. Quy tắc Git để không đè code lên nhau

- Mỗi người làm việc trên 1 branch riêng: `feature/data-pipeline`, `feature/ml-model`, `feature/streaming`, `feature/dashboard`
- Không bao giờ `push` thẳng lên `main`
- Tạo Pull Request để 1 người khác review trước khi merge vào `main` (đúng tinh thần "test chéo" đã thống nhất)
- Commit message rõ ràng: `[data] xử lý missing values cho weather.csv`, `[ml] thêm GBT Regressor`...
- Trước khi bắt đầu code mỗi buổi, `git pull` từ `main` để lấy thay đổi mới nhất của người khác (đặc biệt là `config.py` và schema)

---

## 5. Thứ tự hoàn thành (không thể làm ngược)

```
Task 1 (Hưng) ──▶ Task 2 (Mai) ──▶ Task 3 (Nguyên) ──▶ Task 4 (Nhật)
```
Mỗi người nên có ít nhất 1 bản nháp/mock data sớm để người sau không phải chờ hoàn toàn — ví dụ Hưng có thể gửi trước 1 file mẫu nhỏ (100 dòng, đúng schema) để Mai code thử trong lúc Hưng hoàn thiện phần EDA đầy đủ.

---

## 6. Quy định chung của nhóm

### 6.1. Deadline từng Task (điền ngày cụ thể trước khi bắt đầu)
| Task | Người | Hạn chót | Trạng thái |
|---|---|---|---|
| Task 1 — Data Pipeline | Hưng | __ / __ | ☐ |
| Task 2 — ML Model | Mai | __ / __ | ☐ |
| Task 3 — Streaming | Nguyên | __ / __ | ☐ |
| Task 4 — Dashboard | Nhật | __ / __ | ☐ |
| Tích hợp + Test chéo | Cả nhóm | __ / __ | ☐ |
| Hoàn thiện báo cáo/slide | Cả nhóm | __ / __ | ☐ |

**Nguyên tắc đặt deadline:** deadline Task 1 phải sớm hơn Task 2 ít nhất 2-3 ngày (không đặt cùng ngày), vì Task 2 cần input từ Task 1 mới bắt đầu code thật được — tương tự cho Task 2→3, Task 3→4.

### 6.2. Check-in tiến độ
- Báo cáo ngắn trong group chat **mỗi 2 ngày**: đang làm gì / đã xong gì / đang vướng gì.
- Nếu 1 người trễ deadline **quá 2 ngày** mà không báo trước, người phụ trách Task tiếp theo có quyền yêu cầu nhóm trưởng can thiệp (chuyển việc hoặc hỗ trợ) để không kéo trễ cả dây chuyền.

### 6.3. Khi bị "block" (chờ người trước chưa xong)
- Không ngồi chờ không làm gì — dùng **mock data** (dữ liệu giả tự tạo, đúng schema mục 3) để code thử phần của mình trước, sau đó thay bằng dữ liệu thật của Task trước khi có.
- Báo ngay trong group chat nếu bị block quá 1 ngày, đừng để đến sát deadline mới nói.

### 6.4. Coding convention (thống nhất để đọc code của nhau dễ hơn)
- Tên biến/hàm: tiếng Anh, `snake_case` (ví dụ: `clean_data`, `train_model`), không dùng tiếng Việt có dấu trong code.
- Comment giải thích bằng tiếng Việt hoặc tiếng Anh đều được, miễn nhất quán trong file của mình.
- Mỗi thư mục (`data_pipeline/`, `ml_model/`...) phải có `README.md` riêng ghi rõ: cách chạy code, input cần gì, output ra gì.
- Không commit file dữ liệu lớn (.csv, .parquet gốc) lên Git — thêm vào `.gitignore`, chỉ commit code.

### 6.5. Môi trường chạy thống nhất
- Cùng 1 phiên bản Python (ví dụ 3.10), cùng 1 phiên bản Spark/Kafka (ghi rõ trong `docker-compose.yml` của Task 4).
- Mỗi thư mục có file `requirements.txt` liệt kê thư viện cần cài, để người khác `pip install -r requirements.txt` là chạy được, không phải tự đoán thiếu thư viện gì.

### 6.6. Xử lý xung đột / bất đồng
- Nếu 2 người bất đồng về kỹ thuật (ví dụ chọn model nào), quyết định bằng **số liệu** (so RMSE) chứ không quyết theo cảm tính.
- Nếu bất đồng về phân công/deadline, đưa ra biểu quyết cả nhóm, nhóm trưởng quyết định cuối cùng nếu chia đều 2-2.

### 6.7. Trước buổi bảo vệ/demo
- Chạy thử **toàn bộ pipeline từ đầu đến cuối ít nhất 1 lần** trước ngày bảo vệ, không để đến hôm đó mới demo lần đầu.
- Mỗi người tự chuẩn bị trả lời phần mình làm — nhưng cả nhóm phải hiểu được tổng thể (đọc file này là đủ để nắm sơ bộ phần người khác).
