# dashboard — Task 4 (Nhật)

Dashboard Streamlit đọc bảng **Gold** của Task 3 và hạ tầng Kafka dùng chung cho cả nhóm.

| | Đường dẫn / hằng số (`common/config.py`) |
|---|---|
| Input | `ENERGY_RESULTS_PATH` = `data/gold/energy_results` (đổi bằng biến môi trường cùng tên) |
| Ngưỡng riêng từng đồng hồ (để vẽ dải) | `METER_THRESHOLDS_PATH` (Task 2); thiếu thì dùng `THRESHOLD_RATIO` |
| Throughput / độ trễ | `data/gold/batch_metrics.csv` do `streaming_job.py` ghi |
| Cửa sổ đọc | `DASHBOARD_WINDOW_HOURS` = 168 giờ replay gần nhất |
| Nhịp | `DASHBOARD_REFRESH_SEC` = 5, `STALE_AFTER_SEC` = 30, `TOP_N` = 50, `EVENT_LOG_LIMIT` = 100 |
| Hạ tầng | `infra/docker-compose.yml`: Kafka 4.2 KRaft, `localhost:9094`, topic `energy-readings` |

Dashboard chỉ đọc, đọc Delta bằng `deltalake` (delta-rs), không cần Spark.

## Hạ tầng Kafka

```bash
make infra-up     # docker compose -f infra/docker-compose.yml up -d --wait
make infra-down
make reset        # demo mới: tắt Kafka, xóa data/gold + data/checkpoints, bật lại Kafka
```

| Từ đâu | Địa chỉ |
|---|---|
| Trong Docker | `kafka:9092` |
| Trên máy host | `localhost:9094` (`KAFKA_BOOTSTRAP`) |

Không có Zookeeper (Kafka 4.x chỉ còn KRaft), không có container Spark: Spark chạy `local[*]` trên
máy và đọc/ghi thẳng `data/`. Topic do service `kafka-init` tạo. Kafka không có volume nên mỗi lần
`up` là một phiên mới, vì vậy Kafka, checkpoint và Gold phải reset cùng nhau (`make reset`), nếu
không Spark báo lỗi offset.

## Chạy dashboard

```bash
make dashboard    # = streamlit run dashboard/app.py, http://localhost:8501 (chạy từ gốc repo)
make demo-mock    # dữ liệu giả, không cần Kafka/Spark (mock_stream.py ghi 1 batch mỗi 5 s)
```

Chạy từ gốc repo để Streamlit nhận theme ở `.streamlit/config.toml`. Sau khi sửa `logic.py` hoặc
`view.py`, khởi động lại Streamlit (server giữ module cũ trong bộ nhớ).

## Giao diện

Tổ chức theo tầng để theo dõi được ~1.400 đồng hồ điện:

1. **Tổng quan** (khung tối): số đồng hồ ANOMALY / NORMAL / STALE, thời điểm dữ liệu mới nhất,
   throughput và độ trễ của micro-batch gần nhất.
2. **Sites**: mỗi site một dòng, site có bất thường lên đầu.
3. **Top 50 meters**: lọc theo trạng thái và site; sắp ANOMALY → STALE → NORMAL, trong mỗi nhóm
   residual lớn nhất lên đầu. Thanh residual có dải xanh là **ngưỡng riêng** của đồng hồ đó.
4. **Actual vs baseline**: chọn site → tòa nhà; biểu đồ 168 giờ gần nhất, dải ±ngưỡng riêng, chấm
   đỏ là giờ `is_anomaly`.
5. **Anomaly events**: 100 bản ghi bất thường mới nhất (cả hệ thống hoặc chỉ đồng hồ đang chọn).

Màu và bố cục theo mockup "Energy Dashboard Final". `view.py` dựng HTML và biểu đồ Altair,
`logic.py` xử lý dữ liệu (không dính Streamlit), `app.py` chỉ nối hai phần.

## Đọc dữ liệu

- Mỗi lần refresh chỉ đọc lại khi bảng Delta có version mới (`table_key`).
- Chỉ đọc **168 giờ replay gần nhất**: timestamp mới nhất lấy từ thống kê file trong Delta log
  (không đọc dữ liệu), file nào nằm ngoài khoảng đó bị bỏ qua. Bộ nhớ không tăng theo thời gian chạy
  (~1.400 đồng hồ × 168 giờ ≈ 235 nghìn dòng).
- `is_anomaly` lấy nguyên từ Gold (quy tắc ngưỡng riêng + 3 giờ liền của Task 3), dashboard không
  tính lại.

## STALE

`timestamp` là dữ liệu lịch sử được replay (2017), nên không dùng được để biết feed còn sống.
Dashboard ghi lại thời điểm (đồng hồ của dashboard) lần cuối thấy `timestamp` mới nhất của từng
đồng hồ thay đổi; quá 30 s là STALE. Mọi đồng hồ STALE thì hiện banner đỏ. Hệ quả: sau khi khởi động
lại dashboard, đồng hồ đã ngừng gửi chỉ bị đánh dấu STALE sau 30 s đầu tiên.

## Test

```bash
pytest dashboard    # logic, view, mock, và chạy thật app.py bằng streamlit AppTest
```
