# Báo cáo kiểm thử — Task 3 Streaming

Người kiểm thử độc lập (không viết code). Ngày chạy: **2026-10-18**, branch `feature/streaming` (commit `16e3209`).
Các yêu cầu lấy từ `TASKS.md` (Task 3, mục 3 “Hợp đồng dữ liệu”), `docs/INTEGRATION_GUIDELINES.md` (mục 2: #4, #7, #9, #12) và `config.py`.

## Môi trường

| | |
|---|---|
| Máy | Windows 11 Home, 12 core, Java 17.0.20.1 (Temurin), winutils tại `C:\Users\ADMIN\hadoop` |
| Python | 3.12.1 (`.venv`). Lưu ý: nhóm đã chốt 3.11, nhưng mọi test vẫn chạy được trên 3.12 |
| Thư viện | pyspark 4.2.0, delta-spark 4.4.0, deltalake 1.6.6, confluent-kafka 2.16.0, pyarrow 26.0.0, numpy 2.5.4, pytest 9.1.1 (đúng như `requirements.txt`) |
| Kafka | `apache/kafka:4.2.0` (KRaft) trong Docker, `localhost:9094`, tắt auto-create topic |
| Dữ liệu | Mock: `mock_data/clean_data` (3 site × 4 tòa nhà, 2016-12-01..2017-01-31) + `mock_data/baseline_model` (RandomForest) |

## Cách chạy test

```bash
cd streaming
export HADOOP_HOME='C:\Users\ADMIN\hadoop' PATH="/c/Users/ADMIN/hadoop/bin:$PATH"   # Git Bash
../.venv/Scripts/python -m pytest -q                      # tất cả (~3,5 phút, cần Kafka)
../.venv/Scripts/python -m pytest -q -m "not integration" # chỉ unit test (~1 phút, không cần Kafka)
```

Nếu không kết nối được Kafka, các test `integration` sẽ bị **skip** và in rõ lý do. Nếu thiếu `HADOOP_HOME` thì các test Spark cũng bị skip.
Test end-to-end dùng 1 topic riêng (`test-energy-<uuid>`, xóa sau khi chạy) và thư mục tạm của pytest, nên **không đụng vào** `streaming/output/` hay topic `energy-readings`.

**Kết quả lần kiểm thử đầu:** `47 passed, 1 xfailed in 207.71s` (phần unit: `34 passed, 14 deselected in 56.95s`).
Số dòng `file:line` trong báo cáo ứng với commit `16e3209`.

### Sau khi sửa (người phát triển, cùng ngày)

**Kết quả hiện tại:** `49 passed in 181.67s`. Lỗi #1 đã được sửa nên test xfail nay pass; thêm 1 test cho lỗi #2.

| Mục | Đã sửa |
|---|---|
| Lỗi #1 | `monitor()` tạo thư mục cha của `batch_metrics.csv` trước khi mở file |
| Lỗi #2 | Thêm `parse_utc()`: `--start` không có múi giờ thì coi là UTC, có offset thì đổi sang UTC. Test: `test_parse_utc_keeps_naive_as_utc_and_converts_offsets` |
| Spark treo khi khởi động | Đã tìm ra nguyên nhân gốc: jar của `spark.jars.packages` được tải lại qua RPC cho executor local ở mỗi lần khởi động, và việc tải này đôi khi treo trên Windows. `spark_session.with_jars()` nay chỉ tải jar lần đầu vào `~/.ivy2-bdg2`, các lần sau truyền jar dạng `local:///...` nên không tải lại. Kiểm chứng: 10/10 lần khởi động thành công, mỗi lần khoảng 9 s (trước là khoảng 15 s và thỉnh thoảng treo) |
| README | Ghi chú lấy compose file từ nhánh `dev/task4` cho đến khi Task 4 merge; thêm dòng sự cố về cache jar |

| File | Số test | Nội dung |
|---|---|---|
| `tests/conftest.py` | — | sys.path, 1 SparkSession dùng chung, kiểm tra Kafka → skip, tạo/xóa topic |
| `tests/test_producer.py` | 16 | hàm thuần + producer giả (thread, nhịp tick, spike), báo lỗi sớm |
| `tests/test_streaming_job.py` | 18 | parse / score / batch_metrics trên DataFrame tĩnh, model giả (`SQLTransformer`) + model mock thật |
| `tests/test_end_to_end.py` | 14 (`integration`) | chạy 2 script thật: producer → Kafka → job → Gold, kill + restart, message lỗi, OPTIMIZE |

## Kết quả theo yêu cầu

| # | Yêu cầu | Code | Test / kiểm tra tay | Kết quả | Ghi chú |
|---|---|---|---|---|---|
| 1a | “mỗi thread phụ trách 1 site … mỗi tick gửi lô bản ghi của tất cả tòa nhà trong site đó tại cùng 1 timestamp” (TASKS Task 3) | `kafka_producer.py:95-116`, `169-175` | `test_each_tick_sends_all_buildings_of_a_site_at_same_hour` | PASS | Các site luôn cùng 1 giờ replay (tick k = start + k giờ) |
| 1b | “đều đặn (2–3s/tick)”, GUIDELINES #7 | `kafka_producer.py:102`, `134`; `config.py` `PRODUCER_INTERVAL_SEC=3` | `test_ticks_are_evenly_paced`, `test_producer_interval_is_2_to_3_seconds`, `test_defaults_come_from_config`, `test_kafka_messages_keyed_by_building_and_paced` | PASS | Lịch tính từ t0 chung nên không bị trôi (sai lệch < 0,1 s với producer giả, < 0,25 s qua Kafka thật) |
| 2 | “chỉ lấy giai đoạn test 2017” (`TEST_START`) | `kafka_producer.py:41-47`, `132` | `test_load_site_keeps_only_one_site_and_test_period`, `test_load_site_on_spark_written_mock_silver` | PASS | Nhưng xem lỗi #2 khi `--start` có múi giờ |
| 3 | “1-2 tòa nhà tự tăng đột biến sau X tick” (live demo) | `kafka_producer.py:56-63`, `107-108` | `test_pick_spike_buildings_*`, `test_spike_hits_only_chosen_buildings_from_spike_tick_on`, `test_gold_values_match_silver_with_utc_timestamps` | PASS | Chỉ tòa nhà được chọn bị nhân hệ số, từ tick `--spike-after`; các tòa khác giữ nguyên giá trị Silver |
| 4 | Message JSON đúng schema Silver (có `site_id`), key = `building_id`, timestamp UTC | `kafka_producer.py:66-70` | `test_to_message_is_silver_json_keyed_by_building`, `test_producer_message_round_trip`, `test_parse_timestamp_is_utc`, `test_kafka_messages_keyed_by_building_and_paced` | PASS | Timestamp dạng `2017-01-01T05:00:00+00:00`, Spark đọc ra đúng giờ UTC (so bằng epoch) |
| 5a | Báo lỗi sớm, dễ đọc khi thiếu Kafka / topic / Silver | `kafka_producer.py:119-126`, `149-150` | `test_missing_silver_fails_fast`, `test_unreachable_kafka_fails_fast`, `test_producer_fails_fast_on_missing_topic` | PASS | Kafka không chạy: dừng sau ~10 s (timeout của `list_topics`) |
| 5b | 1 thread site bị crash → dừng tất cả, exit ≠ 0 | `kafka_producer.py:113-116`, `187-188` | `test_crash_in_one_site_stops_all_sites`, `test_crashing_site_thread_exits_non_zero` | PASS | Script thật trả exit 1 với log `site B crashed, stopping all sites` |
| 6 | “parse JSON theo schema Silver”; GUIDELINES #9: message lỗi không làm job crash | `streaming_job.py:43-55`, `91` | `test_parse_valid_message_uses_silver_schema`, `test_bad_messages_are_invalid_and_do_not_crash` (10 kiểu lỗi), `test_invalid_rows_are_dropped_before_scoring`, `test_bad_messages_are_dropped_and_counted`, `test_job_survives_a_batch_of_only_bad_messages` | PASS | Không phải JSON, rỗng, null, mảng, thiếu field, field null, sai kiểu, timestamp sai, JSON cụt → `is_valid=false`, bị bỏ và đếm ở cột `invalid` |
| 7a | “load model Task 2 → baseline, residual, is_anomaly (ngưỡng từ config.py, không hardcode)” | `streaming_job.py:58-72` | `test_residual_and_anomaly_formula`, `test_threshold_is_imported_not_hardcoded`, `test_real_mock_model_scores_test_period` | PASS | `THRESHOLD_RATIO` chính là object của `config`; số `0.28` không xuất hiện trong code job |
| 7b | Ngưỡng ở biên, prediction âm, `--log-target` | `streaming_job.py:65-70` | `test_threshold_boundary_is_strict`, `test_negative_prediction_is_clamped_to_zero`, `test_log_target_applies_expm1` | PASS | Đúng bằng ngưỡng → **không** phải anomaly (dấu `>`); prediction âm → baseline 0; `expm1` được áp dụng |
| 8 | Gold đúng `ENERGY_RESULTS_SCHEMA`, Delta, partition `site_id`, đọc được bằng `deltalake` | `streaming_job.py:71`, `95-101` | `test_gold_schema_matches_contract`, `test_gold_schema_partitioning_readable_with_deltalake` | PASS | Tên, thứ tự, kiểu khớp hoàn toàn; có thư mục `site_id=Fox/Panther/Rat`; đọc có lọc partition không cần Spark |
| 9 | GUIDELINES #4: checkpoint `streaming/output/_checkpoints/energy_results`; restart không ghi trùng, chạy tiếp | `streaming_job.py:98`, `163`; `config.py` | `test_checkpoint_location_is_the_agreed_one`, `test_checkpoint_at_given_location_and_real_output_untouched`, `test_restart_continues_without_duplicates`, `test_gold_row_count_equals_messages_sent` | PASS | Kill cứng job, gửi thêm 72 message + 3 message lỗi, rồi khởi động lại với cùng checkpoint: Gold = 216 dòng, không trùng `(building_id, timestamp)` |
| 10 | “Ghi lại throughput, độ trễ mỗi micro-batch” | `streaming_job.py:104-121`, `131-156` | `test_batch_metrics_*` (3), `test_metrics_csv_has_every_batch` | PASS | Trigger rỗng → `None`; tổng `rows` trong CSV = tổng message (kể cả lỗi), kể cả sau khoảng idle > 10 s. Xem lỗi #1 (đường dẫn CSV) |
| 11 | End-to-end: mock Silver → producer → Kafka → job → Gold; số dòng = số message; tòa nhà spike bị đánh dấu | cả 2 script | `test_end_to_end.py` (cả module) + chạy tay theo README | PASS | Tòa nhà spike `Fox_education_Dan`: 6/6 giờ sau tick spike là anomaly, trước đó phần lớn bình thường |
| 11b | OPTIMIZE định kỳ (`--compact-every`) | `streaming_job.py:124-128`, `154-155` | `test_compaction_keeps_gold_readable` | PASS | Có commit `OPTIMIZE`, Gold vẫn đủ dòng, không trùng |
| 12a | README: hướng dẫn chạy đúng | `README.md` | Chạy tay đúng lệnh “Với dữ liệu giả” (thêm `--topic/--output/--checkpoint` để cô lập) | PARTIAL | Lệnh Python chạy đúng (job sẵn sàng sau 28 s, producer 30 giờ → exit 0 sau 87,7 s, Gold 360/360). **Lệnh `docker compose -f ../dashboard/docker-compose.yml …` không chạy được**: file đó rỗng trên branch này (xem mục “Tài liệu”) |
| 12b | `requirements.txt` đủ thư viện | `requirements.txt` | So các `import` với file + `pip freeze` | PASS | Đủ, version khớp với venv; có `-r ../requirements-spark.txt` |
| 12c | Không hardcode đường dẫn / ngưỡng / Kafka | `streaming/*.py` | `grep` 0.28, 9094, localhost, energy-readings, các đường dẫn | PASS | Mọi giá trị đều lấy từ `config.py`; chỉ có `mock_inputs.py` dùng đường dẫn mock riêng (hợp lý) |
| 12d | Không commit file sinh ra | `.gitignore`, `streaming/.gitignore` | `git status --ignored` | PASS | Chỉ còn `streaming/tests/` (và file báo cáo này) chưa track; `mock_data/`, `output/`, `__pycache__`, `.pytest_cache` đều bị ignore |
| — | GUIDELINES #12: baseline ≈ 0 báo động giả | (quyết định của Task 2) | Chạy tay, xem Gold | N/A | Đúng như README cảnh báo: `Panther_public_Tiny_Kiosk` (baseline TB 1,6 kWh) bị báo anomaly 24/30 giờ dù không có spike |

## Lỗi phát hiện

| # | Mức độ | Vị trí | Mô tả |
|---|---|---|---|
| 1 | Thấp | `streaming_job.py:133-136` | `monitor()` mở `<output>/../batch_metrics.csv` ngay sau `query.start()`, nhưng thư mục cha của `--output` chưa chắc đã tồn tại → job chết với `FileNotFoundError`. |
| 2 | Thấp | `kafka_producer.py:157` | `--start` có múi giờ bị gắn nhãn lại thành UTC (`.replace(tzinfo=utc)`) thay vì đổi sang UTC (`.astimezone(utc)`). |

**Lỗi #1.** Test `test_job_runs_when_output_folder_does_not_exist_yet` (`xfail(strict=True)`).
- Tái hiện: `python streaming_job.py --model mock_data/baseline_model --output X/gold/energy_results --checkpoint X/ckpt` (X là thư mục mới).
- Mong đợi: job chạy bình thường. Thực tế: `FileNotFoundError: ...\X\gold\batch_metrics.csv`, job dừng.
- Với đường dẫn mặc định thì **không** bị lỗi: Spark tạo checkpoint `streaming/output/_checkpoints/...` trước, nên `streaming/output/` đã có sẵn (đã kiểm tra trên một thư mục mới hoàn toàn). Lỗi chỉ xảy ra khi Gold và checkpoint nằm ở 2 thư mục cha khác nhau.
- Cách sửa gợi ý: `metrics_path.parent.mkdir(parents=True, exist_ok=True)` trước khi `open`.

**Lỗi #2.**
- Tái hiện: `--start 2017-01-01T07:00:00+07:00` (tức 00:00 UTC) → replay bắt đầu từ **07:00 UTC**, sai 7 giờ, không có cảnh báo nào. Đã kiểm chứng bằng Python, không viết test vì giá trị mặc định (`TEST_START`, không có múi giờ) vẫn đúng.

**Quan sát (đúng thiết kế nhưng cần biết):**
- `kafka_producer.py:68`: chỉ cần 1 dòng Silver có `meter_reading = null` là `None * factor` gây `TypeError`, toàn bộ producer dừng với exit 1 (đã có test). Theo hợp đồng thì Task 1 đã loại các dòng này, nên đây là cách “báo lỗi to” có chủ ý.
- Nếu Silver còn `air_temperature`/`dew_temperature` null (ví dụ giờ đầu của một site, chưa có giá trị để forward-fill) thì message bị coi là invalid và **bị bỏ** (chỉ được đếm). Khi đó Gold sẽ có ít dòng hơn số message đã gửi.

## Tài liệu / hạ tầng

- `dashboard/docker-compose.yml` **rỗng (0 byte)** trên branch này (`docker compose ... config` báo `empty compose file`). `README.md` dòng 44, 72, 74 và thông báo lỗi ở `kafka_producer.py:124` đều trỏ tới file này, nên làm theo README thì không khởi động được Kafka. Kafka dùng để test là một bản compose tạm của người phát triển (KRaft, `apache/kafka:4.2.0`). Cần chờ Task 4 merge, hoặc ghi chú tạm trong README.
- README nên thêm vào bảng “Sự cố thường gặp”: Spark đôi khi **treo lúc khởi động** (xem rủi ro bên dưới) → tắt rồi chạy lại.

## Rủi ro / chưa kiểm được

- **Spark treo khi khởi động (môi trường Windows):** 2 trong khoảng 15 lần tạo SparkSession bị treo vô thời hạn (> 7 phút). Thread dump cho thấy SparkContext kẹt ở `Executor.updateDependencies → NettyRpcEnv$FileDownloadChannel.read`, tức executor không tải được jar Delta/Kafka, dù đã đặt `127.0.0.1` (`spark_session.py:29-39`). Những lần còn lại khởi động mất ~15 s. Rủi ro **trung bình** cho buổi demo: nên khởi động job trước và chờ dòng log `streaming ...`.
- **Chưa test với dữ liệu thật** (~1.600 tòa nhà, 19 site, cả năm 2017): chưa đo được throughput, độ trễ, RAM của producer (producer load toàn bộ giai đoạn replay vào pyarrow trước khi gửi).
- **Batch đầu tiên quá lớn:** `startingOffsets=earliest` và không có `maxOffsetsPerTrigger` (`streaming_job.py:81`), nên sau thời gian dừng dài, batch đầu tiên sẽ xử lý toàn bộ phần tồn đọng.
- **Exactly-once khi kill giữa lúc đang ghi một batch:** mới kiểm được trường hợp kill khi đang idle, vì kill đúng lúc đang ghi khó tái hiện ổn định. Trên lý thuyết Delta sink vẫn idempotent theo `txnAppId/epochId`.
- Ctrl+C (dừng nhẹ nhàng) chưa test tự động trên Windows; test chỉ dùng kill cứng, là trường hợp xấu hơn.
- Model thật của Task 2 chưa có. Nếu `StringIndexer` dùng `handleInvalid="error"` thì một site/primary_use lạ sẽ làm job crash; model mock dùng `keep`.
- Kafka không có volume: sau `docker compose down/up`, checkpoint cũ không còn khớp offset → phải xóa `output/` (README đã ghi, chưa test).

## Số liệu đo (dữ liệu MOCK, 12 tòa nhà × 3 site, không đại diện cho dữ liệu thật)

Chạy tay theo README (mặc định: trigger 5 s, producer 3 s/tick, 30 giờ = 360 message, spike 2 tòa nhà từ tick 10):

| Chỉ số | Giá trị |
|---|---|
| Job sẵn sàng (đến dòng `streaming ...`) | 28,2 s |
| Producer: load Silver / tổng thời gian | 0,2 s / 87,7 s (30 tick × 3 s) |
| Số batch / tổng dòng | 18 batch / 360 dòng = 360 message (0 lỗi) |
| Thời gian xử lý 1 batch (ổn định, batch 2–17) | 2,1 – 5,7 s (thường 2,7 – 3,6 s) |
| Batch đầu (khởi động nguội) | 13,4 s |
| Độ trễ tối đa (gửi → ghi xong Gold) | 4,3 – 10,3 s khi ổn định; 12,6 – 15,0 s ở 2 batch đầu |
| Throughput | 3,4 – 11,6 dòng/s (bị giới hạn bởi nhịp producer: 12 dòng / 3 s) |
| Anomaly | 85/360; 2 tòa nhà spike: 20/20 giờ sau tick 10 |
| Gold | 884 KB, 55 file parquet cho 360 dòng (nhiều file nhỏ → cần `--compact-every`) |

Test end-to-end (trigger 2 s, producer 0,5 s/tick): batch 132 dòng mất 5,0 s (26,4 dòng/s); batch đầu sau restart (75 dòng gồm 3 lỗi, tồn đọng lúc job dừng) mất 17,7 s, độ trễ 49 s. Ở trigger 2 s job không theo kịp (batch 3–5 s), còn với trigger mặc định 5 s thì theo kịp.
