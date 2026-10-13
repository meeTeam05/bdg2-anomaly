# bdg2-anomaly — Phân công công việc Giai đoạn 1

**Tên đề tài:** Giám sát và phát hiện bất thường tiêu thụ năng lượng tòa nhà thương mại theo thời gian thực bằng Apache Kafka và Spark từ dữ liệu IoT (Building Data Genome 2)

**Nhóm:** Nguyễn Quốc Hưng (23139019) · Phan Đặng Quỳnh Mai (23139028) · Trần Trọng Nguyên (23139030) · Lê Quang Minh Nhật (23139031)

**Mục tiêu của project:** Xây dựng **hoàn chỉnh 1 pipeline Big Data end-to-end bằng Spark** trên **toàn bộ dataset BDG2** — từ ingestion dữ liệu thô, làm sạch, huấn luyện model, đến xử lý streaming thời gian thực và hiển thị. Trọng tâm chấm điểm là pipeline chạy trọn vẹn, đúng kiến trúc Big Data; độ chính xác model là thứ yếu.

**Ý tưởng tổng quát:** Hệ thống học một mô hình Regression để biết "mức tiêu thụ năng lượng BÌNH THƯỜNG của 1 tòa nhà nên là bao nhiêu" (baseline), sau đó mô phỏng dữ liệu các tòa nhà gửi liên tục qua Kafka, dùng Spark Structured Streaming so sánh giá trị THỰC TẾ với baseline đó — nếu lệch (residual) quá ngưỡng thì cảnh báo bất thường (nghi lãng phí điện / thiết bị hỏng), hiển thị lên dashboard.

> **Đọc kèm `docs/INTEGRATION_GUIDELINES.md`** — file đó chốt chi tiết version stack (Python/Spark/Delta/Kafka), cấu trúc `config.py`, và liệt kê các điểm cả nhóm cần thống nhất thêm trước khi tích hợp. File này (`TASKS.md`) là luật chơi chung; `INTEGRATION_GUIDELINES.md` là phụ lục kỹ thuật, không thay thế.

---

## 0. Nguyên tắc & kiến trúc tổng thể

**Nguyên tắc bắt buộc:**
1. **Dùng toàn bộ dataset** — tất cả tòa nhà, tất cả site, đủ 2 năm (2016–2017) của đồng hồ điện (`electricity`). Không lọc bớt tòa nhà ở bất kỳ bước nào của pipeline batch.
2. **Mọi bước xử lý dữ liệu đều bằng Spark** (Spark SQL / DataFrame / MLlib / Structured Streaming). Không dùng pandas để xử lý dữ liệu chính. Ngoại lệ duy nhất: dashboard (Task 4) đọc kết quả bằng `deltalake` để hiển thị.
3. **Mọi bảng trung gian đều là Delta Lake**, tổ chức theo kiến trúc **Medallion (Bronze → Silver → Gold)**.

**Kiến trúc pipeline:**

```
 BATCH (Spark)                                          STREAMING (Kafka + Spark)
 ─────────────                                          ─────────────────────────
 CSV gốc BDG2 (toàn bộ)                                 Kafka Producer (replay Silver, giai đoạn test)
     │  [Task 1] Ingestion                                   │
     ▼                                                       ▼
 [Bronze] Delta — dữ liệu thô, giữ nguyên              Spark Structured Streaming  [Task 3]
     │  [Task 1] Unpivot + join + làm sạch + feature         │  load model → baseline, residual, is_anomaly
     ▼                                                       ▼
 [Silver] Delta — clean_data (hợp đồng mục 3)           [Gold] Delta — energy_results
     │  [Task 2] Spark MLlib Pipeline                        │
     ▼                                                       ▼
 PipelineModel (baseline_model) ─────────────────────▶  Dashboard (Streamlit)  [Task 4]
```

**Bằng chứng "Big Data" phải có trong báo cáo** (mỗi người ghi lại số liệu phần của mình): số dòng ở mỗi tầng (Bronze/Silver/Gold), dung lượng, số partition, thời gian chạy từng job, cấu hình Spark (driver/executor memory, số core). Volume dữ liệu ở mức vừa phải (vài chục triệu dòng) — khi trình bày, nhấn mạnh **Velocity** (streaming thời gian thực) và **kiến trúc có khả năng mở rộng**.

---

## 1. Cấu trúc thư mục (mỗi người CHỈ code trong thư mục của mình)

```
bdg2-anomaly/
├── data_pipeline/        ← Hưng
│   ├── ingest.py          (CSV → Bronze)
│   ├── eda.ipynb
│   ├── preprocess.py      (Bronze → Silver)
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
├── config.py               ← chung (ngưỡng, schema)
└── README.md               ← tổng quan cả repo
```

**Quy tắc:** không sửa file trong thư mục của người khác. Nếu cần đổi (ví dụ đổi tên cột), phải báo trong group chat trước, vì các thư mục phụ thuộc lẫn nhau qua "hợp đồng dữ liệu" ở mục 3.

---

## 2. Task chi tiết theo người

### Task 1 — Data Pipeline: Ingestion → Bronze → Silver (Hưng)
**Input:** File gốc BDG2 (`electricity.csv`, `weather.csv`, `metadata.csv`) — **toàn bộ**, không lọc — đặt tại `data_pipeline/raw/` (không commit lên Git).
**Việc làm:**
- **Ingestion (`ingest.py`):** đọc 3 file CSV bằng Spark, ghi nguyên trạng ra Delta tầng Bronze tại `data_pipeline/output/bronze/{electricity,weather,metadata}`. Không biến đổi dữ liệu ở bước này (để có thể chạy lại Silver mà không cần đọc lại CSV).
- **EDA (`eda.ipynb`, dùng Spark):** schema, số dòng, số tòa nhà / site / loại hình (`primary_use`), tỉ lệ thiếu dữ liệu theo site và theo tòa nhà, phân phối `meter_reading`.
- **Bronze → Silver (`preprocess.py`):**
  - `electricity.csv` ở dạng **wide** (mỗi tòa nhà 1 cột) → **unpivot** về dạng long (`building_id`, `timestamp`, `meter_reading`) bằng Spark (`unpivot`/`stack`).
  - Join với `metadata` theo `building_id` (lấy `site_id`, `primary_use`, `square_feet`), rồi join với `weather` theo `site_id` + `timestamp`.
  - Tạo feature: `hour_of_day`, `day_of_week`, `is_weekend`.
  - Làm sạch: loại giá trị âm, giá trị đứng yên bất thường (flatline), xử lý null (weather: nội suy/forward-fill theo site; `meter_reading` null: loại bỏ). Ghi lại số dòng bị loại ở từng bước.
  - Ghi ra Delta, **partition theo `site_id`**.
**Output bắt buộc:** bảng Delta tầng Silver đúng schema ở mục 3, đặt tại `data_pipeline/output/clean_data` (thư mục Delta table, không phải file `.parquet` đơn), chứa **tất cả tòa nhà** qua được bước làm sạch.
**Done khi:** Mai chạy được `train_baseline.py` trên bảng này không lỗi.

### Task 2 — ML Model với Spark MLlib (Mai)
**Input:** bảng Delta `data_pipeline/output/clean_data` (từ Task 1) — toàn bộ tòa nhà.
**Việc làm:**
- Xây **Spark MLlib `Pipeline`** gồm: `StringIndexer` (`primary_use`, `site_id`) → `VectorAssembler` → Regressor. Toàn bộ bước biến đổi feature nằm trong Pipeline để Task 3 load về là dùng được ngay, không phải viết lại.
- Feature: thời gian (`hour_of_day`, `day_of_week`, `is_weekend`), thời tiết (`air_temperature`, `dew_temperature`), đặc trưng tòa nhà (`square_feet`, `primary_use`, `site_id`). **Không** đưa `building_id` trực tiếp làm feature (~1.600 giá trị, vượt `maxBins` của cây).
- Chia train/test **theo thời gian** (không random): **train = năm 2016, test = năm 2017** (mốc `TEST_START` trong `config.py`). Task 3 replay đúng giai đoạn test để model không "nhìn thấy trước" dữ liệu.
- Train + so sánh: Linear Regression, Random Forest, GBT. Có thể thử target `log1p(meter_reading)` vì quy mô tòa nhà chênh lệch lớn.
- Được dùng sample để dò tham số cho nhanh, nhưng **model cuối cùng phải train trên toàn bộ tập train**.
- Đánh giá RMSE/MAE (tổng thể và theo `primary_use`), so với baseline "trung bình cộng". Chọn model tốt nhất theo RMSE.
- **Injected anomaly test (OFFLINE, không trùng với Task 3):** tự tiêm 1 đoạn bất thường giả vào dữ liệu test tĩnh, chạy qua model + ngưỡng 1 lần, kiểm chứng **ngưỡng residual hoạt động đúng về mặt thống kê**. Kiểm tra riêng tỉ lệ báo động giả ở các tòa nhà có tiêu thụ rất nhỏ (baseline gần 0 dễ vượt ngưỡng 28%) và đề xuất cách xử lý nếu cần.
- Ghi lại: số dòng train/test, thời gian train từng model, cấu hình Spark.
**Output bắt buộc:** model tốt nhất lưu tại `ml_model/models/baseline_model/`, load được bằng `PipelineModel.load(...)`, input là đúng các cột của schema Silver (mục 3), output có cột `prediction` (nếu train trên `log1p` thì Task 3 phải biết để `expm1` — ghi rõ trong `ml_model/README.md`).
**Done khi:** Nguyên load được model này trong `streaming_job.py` và ra được `prediction` không lỗi.

### Task 3 — Streaming Pipeline: Kafka → Spark Structured Streaming → Gold (Nguyên)
**Input:** bảng Delta `data_pipeline/output/clean_data` (để replay, chỉ lấy **giai đoạn test 2017**) + model từ Task 2 + **hạ tầng Docker của Task 4 (xem lưu ý thứ tự ở mục 5)**
**Việc làm:**
- Kafka Producer: multithreading, **mỗi thread phụ trách 1 site** (19 site), mỗi tick gửi lô bản ghi của **tất cả tòa nhà trong site đó** tại cùng 1 timestamp, đều đặn (2–3s/tick). Không tạo 1 thread cho mỗi tòa nhà (~1.600 thread là quá nhiều).
- **Kịch bản tăng đột biến (LIVE DEMO, khác với injected anomaly test của Mai ở Task 2):** lập trình sẵn 1-2 tòa nhà tự tăng đột biến sau X tick khi đang chạy streaming — mục đích là **có cái để demo trực quan trước hội đồng**, không phải để kiểm chứng thống kê.
- Spark Structured Streaming: đọc Kafka → parse JSON theo schema Silver → load model Task 2 → tính `baseline`, `residual`, `is_anomaly` (ngưỡng lấy từ `config.py`, không hardcode).
- Ghi kết quả vào Delta tầng Gold, **partition theo `site_id`**, có checkpoint tại `streaming/output/_checkpoints/energy_results`.
- Ghi lại: throughput (bản ghi/giây), độ trễ mỗi micro-batch.
**Output bắt buộc:** bảng Delta Lake tại `streaming/output/energy_results` đúng schema mục 3
**Done khi:** Nhật đọc được bảng này và hiển thị đúng lên dashboard.

### Task 4 — Dashboard & Hạ tầng (Nhật)
**Input:** `streaming/output/energy_results` (từ Task 3, cho phần dashboard)
**Việc làm:**
- `docker-compose.yml`: Kafka + Zookeeper + Spark (dùng chung cho cả nhóm) — **xem mục 5, phần này phải làm và merge SỚM, không đợi đến lượt Task 4 mới bắt đầu**. Cấp đủ RAM cho Spark để chạy được toàn bộ dataset.
- Dashboard (Streamlit) đọc Delta Lake, tự refresh. Vì có ~1.600 tòa nhà, dashboard phải tổ chức theo tầng:
  - **Tổng quan:** số tòa nhà đang giám sát, số bất thường theo site, throughput.
  - **Top-N tòa nhà bất thường** gần nhất.
  - **Chi tiết 1 tòa nhà** (chọn theo site → building): biểu đồ thực tế vs baseline, log sự kiện.
- Chỉ đọc phần dữ liệu cần thiết (lọc theo partition `site_id` / khoảng thời gian), không load toàn bảng mỗi lần refresh.
**Output bắt buộc:** `docker compose up` chạy được hạ tầng; `streamlit run dashboard.py` xem được tại `localhost:8501`
**Done khi:** Cả nhóm demo thử end-to-end không lỗi.

---

## 3. Hợp đồng dữ liệu (Data Contract) — BẮT BUỘC tuân thủ để không lệch nhau

Đây là phần quan trọng nhất giúp 4 người làm song song mà không đụng nhau. Tên cột, kiểu dữ liệu dưới đây là **cố định**, ai muốn đổi phải được cả nhóm đồng ý trước.

**Schema đầu ra Task 1 → Task 2, Task 3 (Silver — `clean_data`):**
| Cột | Kiểu | Ghi chú |
|---|---|---|
| `building_id` | string | |
| `site_id` | string | cột partition |
| `timestamp` | timestamp | UTC |
| `meter_reading` | double | kWh |
| `air_temperature` | double | |
| `dew_temperature` | double | liên quan độ ẩm |
| `hour_of_day` | int | 0–23 |
| `day_of_week` | int | 1–7 |
| `is_weekend` | int | 0/1 |
| `square_feet` | double | |
| `primary_use` | string | |

**Schema đầu ra Task 3 → Task 4 (Gold — `energy_results`):**
| Cột | Kiểu | Ghi chú |
|---|---|---|
| `building_id` | string | |
| `site_id` | string | cột partition |
| `timestamp` | timestamp | UTC |
| `meter_reading` | double | giá trị thực tế |
| `baseline` | double | model dự đoán |
| `residual` | double | `meter_reading - baseline` |
| `is_anomaly` | boolean | `abs(residual) > baseline * 0.28` |

**Ngưỡng dùng chung:** `THRESHOLD_RATIO = 0.28` — khai báo 1 chỗ duy nhất (file `config.py` ở gốc repo), Task 2 và Task 3 cùng import từ đó, không hardcode riêng mỗi nơi. Chi tiết cấu trúc `config.py` (bao gồm `CLEAN_DATA_SCHEMA`, `ENERGY_RESULTS_SCHEMA` dạng code) xem `docs/INTEGRATION_GUIDELINES.md` mục 1.

**Định dạng lưu trữ:** mọi bảng (Bronze, Silver, Gold) đều là **Delta Lake** (không dùng Parquet thuần).

**Đường dẫn các bảng/model** — khai báo trong `config.py` (cột "Hằng số"), mọi task import từ đó, **không hardcode đường dẫn**:
| Tầng | Đường dẫn | Hằng số | Ghi bởi | Đọc bởi |
|---|---|---|---|---|
| CSV gốc | `data_pipeline/raw/` (không commit) | `RAW_DATA_DIR` | — | Task 1 |
| Bronze | `data_pipeline/output/bronze/{electricity,weather,metadata}` | `BRONZE_DIR` | Task 1 | Task 1 |
| Silver | `data_pipeline/output/clean_data` | `CLEAN_DATA_PATH` | Task 1 | Task 2, Task 3 |
| Model | `ml_model/models/baseline_model/` | `MODEL_PATH` | Task 2 | Task 3 |
| Gold | `streaming/output/energy_results` | `ENERGY_RESULTS_PATH` | Task 3 | Task 4 |

---

## 4. Quy tắc Git để không đè code lên nhau

- Mỗi người làm việc trên 1 branch riêng: `feature/data-pipeline`, `feature/ml-model`, `feature/streaming`, `feature/dashboard`
- Không bao giờ `push` thẳng lên `main`
- Tạo Pull Request để 1 người khác review trước khi merge vào `main` (đúng tinh thần "test chéo" đã thống nhất)
- Commit message rõ ràng: `[data] xử lý missing values cho weather.csv`, `[ml] thêm GBT Regressor`...
- Trước khi bắt đầu code mỗi buổi, `git pull` từ `main` để lấy thay đổi mới nhất của người khác (đặc biệt là `config.py` và schema)
- Không commit thư mục `output/` hay dữ liệu Delta lên Git.

---

## 5. Thứ tự hoàn thành

**Lưu ý quan trọng:** thứ tự dưới đây nói về thứ tự **bàn giao dữ liệu/model**, KHÔNG phải thứ tự bắt đầu code. Phần hạ tầng Docker (`docker-compose.yml` — Kafka, Zookeeper, Spark) tuy nằm trong Task 4 nhưng phải được **Nhật làm và merge sớm nhất, ngay từ đầu**, vì Nguyên (Task 3) cần Kafka + Spark chạy được mới test được code streaming của mình.

```
Ngay từ đầu (song song với Task 1):
  Nhật ──▶ viết & merge docker-compose.yml (Kafka + Zookeeper + Spark) TRƯỚC TIÊN

Thứ tự bàn giao dữ liệu/model:
  Task 1 (Hưng): Bronze ──▶ Silver
                              │
                              ▼
                 Task 2 (Mai) ──▶ Task 3 (Nguyên) ──▶ Task 4 phần Dashboard (Nhật)
```

Mỗi người nên có ít nhất 1 bản nháp/mock data sớm để người sau không phải chờ hoàn toàn — ví dụ Hưng có thể gửi trước 1 bảng Silver mẫu (vài site, đúng schema) để Mai code thử trong lúc Hưng xử lý toàn bộ dataset. Tương tự, Nguyên có thể tự tạo 1 model giả (trả về hằng số) để code thử Streaming Job trong lúc chờ Mai train xong model thật. **Mock data chỉ dùng để code thử — bản nộp cuối cùng phải chạy trên toàn bộ dataset.**

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
- Mỗi thư mục (`data_pipeline/`, `ml_model/`...) phải có `README.md` riêng ghi rõ: cách chạy code, input cần gì, output ra gì, và **số liệu quy mô** (số dòng, thời gian chạy) của phần mình.
- Không commit file dữ liệu lớn (.csv gốc, thư mục Delta `output/`) lên Git — thêm vào `.gitignore`, chỉ commit code.

### 6.5. Môi trường chạy thống nhất
**Version đã chốt cho cả nhóm (xem `docs/INTEGRATION_GUIDELINES.md` để biết cách verify):**
| Thành phần | Version |
|---|---|
| Python | 3.11 |
| Java (bắt buộc cho Spark) | 17 |
| pyspark | 4.2.0 |
| delta-spark | 4.4.0 |
| deltalake (Task 4 dùng để đọc, không cần Spark) | 1.6.6 |

- File `requirements-spark.txt` ở gốc repo pin sẵn `pyspark==4.2.0` + `delta-spark==4.4.0` — mỗi `requirements.txt` riêng của từng task thêm dòng `-r ../requirements-spark.txt` thay vì tự ghi version khác.
- Mỗi thư mục có file `requirements.txt` liệt kê thư viện cần cài, để người khác `pip install -r requirements.txt` là chạy được, không phải tự đoán thiếu thư viện gì.
- **Không tự ý đổi version** trong bảng trên khi đang làm giữa chừng — nếu thấy cần đổi (ví dụ gặp bug), phải báo cả nhóm trước, vì đổi version pyspark có thể làm hỏng code của người khác.
- Chạy trên toàn bộ dataset cần tăng bộ nhớ Spark (ví dụ `spark.driver.memory` ≥ 4g) — mỗi người ghi cấu hình đã dùng vào README của mình.

### 6.6. Xử lý xung đột / bất đồng
- Nếu 2 người bất đồng về kỹ thuật (ví dụ chọn model nào), quyết định bằng **số liệu** (so RMSE) chứ không quyết theo cảm tính.
- Nếu bất đồng về phân công/deadline, đưa ra biểu quyết cả nhóm, nhóm trưởng quyết định cuối cùng nếu chia đều 2-2.

### 6.7. Trước buổi bảo vệ/demo
- Chạy thử **toàn bộ pipeline từ đầu đến cuối (CSV gốc → Bronze → Silver → Model → Streaming → Dashboard) ít nhất 1 lần trên toàn bộ dataset** trước ngày bảo vệ, không để đến hôm đó mới demo lần đầu.
- Mỗi người tự chuẩn bị trả lời phần mình làm — nhưng cả nhóm phải hiểu được tổng thể (đọc file này là đủ để nắm sơ bộ phần người khác).
