# dashboard (Task 4: Dashboard và hạ tầng)

Người phụ trách: Nhật.

- **Input:** bảng Delta `streaming/output/energy_results` (Task 3), schema `ENERGY_RESULTS_SCHEMA` trong `config.py`.
- **Output:** hạ tầng Kafka (`docker compose`) và dashboard Streamlit tại `http://localhost:8501`.

Thuật ngữ (Building status, Stale, Anomaly event...) xem `../GLOSSARY.md`. Lý do không có Zookeeper và Spark container xem `../docs/adr/0001-kafka-kraft-no-spark-container.md`.

## Hạ tầng Kafka

```bash
docker compose -f dashboard/docker-compose.yml up -d --wait   # từ gốc repo
```

| Từ đâu | Địa chỉ |
|---|---|
| Trong Docker | `kafka:9092` |
| Trên máy host | `localhost:9094` (`KAFKA_BOOTSTRAP` trong `config.py`) |

Topic `energy-readings` (1 partition) do service `kafka-init` tạo. Kafka không có volume nên mỗi lần `up` là một phiên mới.

**Reset giữa các lần demo:** chạy `dashboard/reset_demo.sh`. Script dừng Kafka, xóa `streaming/output/` (gồm cả checkpoint Spark `streaming/output/_checkpoint/`), rồi bật lại Kafka. Không xóa lẻ từng thứ: Kafka, checkpoint và Delta phải reset cùng nhau, nếu không Spark báo lỗi offset hoặc ghi trùng.

## Dashboard

```bash
uv venv --python 3.11 dashboard/.venv
uv pip install --python dashboard/.venv/bin/python -r dashboard/requirements.txt
cd dashboard
.venv/bin/streamlit run dashboard.py        # http://localhost:8501
```

Đường dẫn bảng đọc từ biến môi trường `ENERGY_RESULTS_PATH` (mặc định `streaming/output/energy_results`). Dashboard chỉ đọc, không ghi vào Delta và không cần pyspark. Giá trị cấu hình (refresh 5s, stale 30s, 200 điểm biểu đồ, 100 sự kiện) nằm trong `config.py`.

Sau khi sửa `logic.py` hoặc `view.py`, khởi động lại Streamlit (server giữ module cũ trong bộ nhớ).

### Giao diện

Theo mockup "Energy Dashboard Final" của bên graphic (file gốc để trong `tmp/`, không commit). `view.py` dựng HTML (header, 4 ô số, bảng, panel tòa chọn, log) và biểu đồ Altair; `dashboard.py` chỉ nối dữ liệu với `view.py`. Theme nằm ở `.streamlit/config.toml`, nên phải chạy `streamlit run` từ thư mục `dashboard/`. Font Geist tải từ Google Fonts, máy demo không có mạng thì tự dùng font hệ thống.

Chữ trên giao diện: ghi chú STALE tính theo đồng hồ dashboard ("no data for 41 s"), banner đỏ khi mọi tòa đều STALE, dải ±28% lấy từ `THRESHOLD_RATIO` trong `config.py`.

## Chạy thử khi Task 3 chưa xong (mock)

```bash
cd dashboard
.venv/bin/python mock_stream.py --reset                    # mỗi 5s ghi 1 batch vào .mock_output/
ENERGY_RESULTS_PATH=$PWD/.mock_output/energy_results .venv/bin/streamlit run dashboard.py
```

Kịch bản mock: `B02` tăng đột biến từ tick 10 (ANOMALY), `B03` ngừng gửi sau tick 8 (STALE sau 30s), `B04` luôn có baseline 0 (hiển thị `n/a`).

## Test

```bash
cd dashboard && .venv/bin/python -m pytest -q
```

## Lưu ý về STALE

`timestamp` là dữ liệu lịch sử được replay (2016-2017), nên không dùng được để biết feed còn sống. Dashboard ghi lại thời điểm (đồng hồ của dashboard) lần cuối thấy `timestamp` mới nhất của từng tòa thay đổi; quá 30s là STALE. Hệ quả: sau khi khởi động lại dashboard, tòa đã ngừng gửi chỉ bị đánh dấu STALE sau 30s đầu tiên.
