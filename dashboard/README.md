# dashboard — Task 4 (Nhật)

Dashboard Streamlit đọc bảng Gold. Hạ tầng Docker dùng chung nằm ở `infra/docker-compose.yml`.

| | Đường dẫn (`common/config.py`) |
|---|---|
| Input | `ENERGY_RESULTS_PATH` = `data/gold/energy_results` (đọc bằng `deltalake`, không cần Spark) |

Dashboard gồm: tổng quan theo site → Top-N tòa nhà bất thường → chi tiết 1 tòa nhà (thực tế vs baseline, log sự kiện). Chỉ đọc partition/khoảng thời gian cần thiết mỗi lần refresh.

## Cách chạy

```bash
# từ gốc repo, sau `make setup`
make infra-up                          # hạ tầng cho cả nhóm
streamlit run dashboard/dashboard.py   # http://localhost:8501
```
