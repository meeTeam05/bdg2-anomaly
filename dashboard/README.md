# dashboard — Task 4 (Nhật)

Hạ tầng Docker dùng chung (Kafka + Zookeeper + Spark) và dashboard Streamlit đọc bảng Gold.

| | Đường dẫn (`config.py`) |
|---|---|
| Input | `ENERGY_RESULTS_PATH` = `streaming/output/energy_results` (đọc bằng `deltalake`, không cần Spark) |

Dashboard gồm: tổng quan theo site → Top-N tòa nhà bất thường → chi tiết 1 tòa nhà (thực tế vs baseline, log sự kiện). Chỉ đọc partition/khoảng thời gian cần thiết mỗi lần refresh.

## Cách chạy

```bash
docker compose up -d          # hạ tầng cho cả nhóm
pip install -r requirements.txt
streamlit run dashboard.py    # http://localhost:8501
```
