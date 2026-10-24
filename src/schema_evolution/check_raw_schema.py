import itertools
import json
import os
import sys

from pyspark.sql import SparkSession

from bronze.load_product_reviews_bronze import (
    configured_data_mode,
    load_dataset_config,
    raw_path,
)
from schema_evolution.baseline import evaluate, load_baseline
from schema_evolution.jsonl_extractor import (
    SOURCE_IGNORED_FIELDS,
    SOURCE_OPAQUE_FIELDS,
    build_profile,
)


# report: chỉ phát hiện và ghi lại, luôn để pipeline chạy tiếp.
# block: còn nguồn nào REVIEW, BLOCK hoặc ERROR thì ghi trạng thái
# SCHEMA_DRIFT_BLOCKED và thoát mã 99; DAG bỏ qua (skip) các bước sau nên Serving
# giữ nguyên phiên bản đang phục vụ.
SUPPORTED_MODES = ("report", "block")
DEFAULT_MODE = "report"
BLOCKED_EXIT_CODE = 99
BLOCKED_STATUS = "SCHEMA_DRIFT_BLOCKED"
DEFAULT_LINES_PER_PART = 20000
# Baseline đã duyệt nằm trong Git (schemas/<nguồn>/vN.json), mount chỉ đọc.
DEFAULT_BASELINE_ROOT = "/opt/project/schemas"
SOURCES = ("reviews", "products")
ERROR = "ERROR"
# REVIEW cũng phải chờ người duyệt; ERROR nghĩa là không kiểm tra được nên không
# được coi là an toàn.
HELD_ACTIONS = ("REVIEW", "BLOCK", ERROR)

MARK_BLOCKED_SQL = """
    UPDATE serving.pipeline_runs
    SET status = 'SCHEMA_DRIFT_BLOCKED',
        finished_at = NOW(),
        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
        error_message = ?,
        updated_at = NOW()
    WHERE run_id = ?
"""

UPSERT_EVENT_SQL = """
    INSERT INTO serving.schema_drift_events (
        run_id, source_name, mode, action, baseline_version, records,
        malformed, sampled_partitions, lines_per_partition, raw_path,
        changes, error_message, checked_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, NOW())
    ON CONFLICT (run_id, source_name) DO UPDATE SET
        mode = EXCLUDED.mode,
        action = EXCLUDED.action,
        baseline_version = EXCLUDED.baseline_version,
        records = EXCLUDED.records,
        malformed = EXCLUDED.malformed,
        sampled_partitions = EXCLUDED.sampled_partitions,
        lines_per_partition = EXCLUDED.lines_per_partition,
        raw_path = EXCLUDED.raw_path,
        changes = EXCLUDED.changes,
        error_message = EXCLUDED.error_message,
        checked_at = NOW()
"""


def configured_mode(value=None):
    mode = (value or os.environ.get("SCHEMA_CHECK_MODE", DEFAULT_MODE)).strip().lower()
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"SCHEMA_CHECK_MODE phải là một trong {list(SUPPORTED_MODES)}")
    return mode


def configured_lines_per_part(value=None):
    raw = value or os.environ.get("SCHEMA_CHECK_LINES_PER_PART", str(DEFAULT_LINES_PER_PART))
    lines = int(raw)
    if lines <= 0:
        raise ValueError("SCHEMA_CHECK_LINES_PER_PART phải lớn hơn 0")
    return lines


def profile_partitions(spark, path, lines_per_partition, opaque_fields, ignored_fields):
    # Mỗi file .gz là một partition (gzip không chia nhỏ được). Lấy N dòng đầu của
    # từng partition, đếm ngay trên executor rồi chỉ gửi số đếm về driver.
    opaque_fields = tuple(opaque_fields)
    ignored_fields = tuple(ignored_fields)

    def profile_partition(lines):
        yield build_profile(
            itertools.islice(lines, lines_per_partition),
            opaque_fields,
            ignored_fields,
        )

    rdd = spark.sparkContext.textFile(path)
    merged = build_profile([], opaque_fields, ignored_fields)
    for profile in rdd.mapPartitions(profile_partition).collect():
        merged.merge(profile)
    return merged.result(), rdd.getNumPartitions()


def error_event(source_name, message, path=None):
    return {
        "source_name": source_name,
        "action": ERROR,
        "baseline_version": None,
        "records": None,
        "malformed": None,
        "sampled_partitions": None,
        "lines_per_partition": None,
        "raw_path": path,
        "changes": [],
        "error_message": message[:4000],
    }


def check_source(spark, dataset, source, data_mode, lines_per_partition, baseline_root):
    name = source["name"]
    path = raw_path(dataset["dataset_id"], source, data_mode)

    # evaluate tự lấy schema đầu tiên làm v1 khi chưa có baseline; ở đây baseline
    # phải là bản đã duyệt nên thiếu thì báo lỗi thay vì lặng lẽ cho qua.
    if load_baseline(name, baseline_root) is None:
        return error_event(name, f"Chưa có baseline đã duyệt trong {baseline_root}/{name}", path)

    observed, partitions = profile_partitions(
        spark, path, lines_per_partition,
        SOURCE_OPAQUE_FIELDS[name], SOURCE_IGNORED_FIELDS[name],
    )
    if observed["records"] == 0:
        return error_event(name, f"Không đọc được dòng JSON hợp lệ nào từ {path}", path)

    # apply=False: không ghi phiên bản mới; baseline chỉ đổi khi người duyệt approve.
    result = evaluate(
        name, observed["schema"], baseline_root,
        dependencies_root=baseline_root, apply=False,
    )
    return {
        "source_name": name,
        "action": result["action"],
        "baseline_version": result["baseline_version"],
        "records": observed["records"],
        "malformed": observed["malformed"],
        "sampled_partitions": partitions,
        "lines_per_partition": lines_per_partition,
        "raw_path": path,
        "changes": result["changes"],
        "error_message": None,
    }


def format_event(event):
    if event["action"] == ERROR:
        return f"[{event['source_name']}] ERROR: {event['error_message']}"

    lines = [
        f"[{event['source_name']}] {event['action']} so với baseline "
        f"v{event['baseline_version']}: {event['records']:,} dòng từ "
        f"{event['sampled_partitions']} partition, {event['malformed']} dòng lỗi định dạng"
    ]
    for change in event["changes"]:
        lines.append(
            f"    {change['action']:<6} {change['severity']:<8} "
            f"{change['change']} {change['field']}: {change['reason']}"
        )
    return "\n".join(lines)


def held_events(events):
    return [event for event in events if event["action"] in HELD_ACTIONS]


def exit_code(mode, events):
    if mode == "block" and held_events(events):
        return BLOCKED_EXIT_CODE
    return 0


def blocked_summary(events):
    # Một dòng cho mỗi nguồn bị giữ, chỉ nêu các thay đổi bị giữ, để ghi vào
    # pipeline_runs.error_message và hiện trong tab Operations.
    parts = []
    for event in held_events(events):
        if event["action"] == ERROR:
            detail = event["error_message"]
        else:
            detail = ", ".join(
                f"{change['change']} {change['field']}"
                for change in event["changes"]
                if change["action"] in HELD_ACTIONS
            )
        parts.append(f"{event['source_name']}: {event['action']} ({detail})")
    return "Schema nguồn khác baseline đã duyệt; " + "; ".join(parts)


def open_postgres_connection(spark):
    driver = "org.postgresql.Driver"
    spark._jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(
        driver
    )
    url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return spark._jvm.java.sql.DriverManager.getConnection(
        url,
        os.environ["POSTGRES_USER"],
        os.environ["POSTGRES_PASSWORD"],
    )


def event_parameters(run_id, mode, event):
    # Thứ tự khớp với các dấu ? trong UPSERT_EVENT_SQL, kèm kiểu SQL để ghi null.
    return [
        (run_id, "VARCHAR"),
        (event["source_name"], "VARCHAR"),
        (mode, "VARCHAR"),
        (event["action"], "VARCHAR"),
        (event["baseline_version"], "INTEGER"),
        (event["records"], "BIGINT"),
        (event["malformed"], "BIGINT"),
        (event["sampled_partitions"], "INTEGER"),
        (event["lines_per_partition"], "INTEGER"),
        (event["raw_path"], "VARCHAR"),
        (json.dumps(event["changes"], ensure_ascii=False, sort_keys=True), "VARCHAR"),
        (event["error_message"], "VARCHAR"),
    ]


def write_events(spark, run_id, mode, events):
    sql_types = spark._jvm.java.sql.Types
    connection = open_postgres_connection(spark)
    try:
        statement = connection.prepareStatement(UPSERT_EVENT_SQL)
        try:
            for event in events:
                for index, (value, sql_type) in enumerate(
                    event_parameters(run_id, mode, event), start=1
                ):
                    if value is None:
                        statement.setNull(index, getattr(sql_types, sql_type))
                    elif sql_type == "VARCHAR":
                        statement.setString(index, value)
                    elif sql_type == "INTEGER":
                        statement.setInt(index, int(value))
                    else:
                        statement.setLong(index, int(value))
                statement.executeUpdate()
        finally:
            statement.close()
    finally:
        connection.close()


def mark_run_blocked(spark, run_id, message):
    connection = open_postgres_connection(spark)
    try:
        statement = connection.prepareStatement(MARK_BLOCKED_SQL)
        try:
            statement.setString(1, message[:4000])
            statement.setString(2, run_id)
            if statement.executeUpdate() != 1:
                raise RuntimeError(f"No pipeline run row found for {run_id}")
        finally:
            statement.close()
    finally:
        connection.close()


def main():
    mode = configured_mode()
    lines_per_partition = configured_lines_per_part()
    data_mode = configured_data_mode()
    orchestration_run_id = os.environ.get("ORCHESTRATION_RUN_ID")
    run_id = orchestration_run_id or "manual-schema-check"
    baseline_root = os.environ.get("SCHEMA_BASELINE_ROOT", DEFAULT_BASELINE_ROOT)
    dataset, sources = load_dataset_config()

    spark = SparkSession.builder.appName("check-raw-schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    events = []
    try:
        for name in SOURCES:
            try:
                event = check_source(
                    spark, dataset, sources[name], data_mode,
                    lines_per_partition, baseline_root,
                )
            except Exception as error:
                # Report không được làm dừng pipeline nên chỉ ghi lại. Block để lỗi
                # nổi lên: task thất bại, Airflow retry (lỗi có thể chỉ tạm thời,
                # ví dụ MinIO chưa sẵn sàng) và run thành FAILED nếu vẫn lỗi.
                if mode == "block":
                    raise
                event = error_event(name, f"{type(error).__name__}: {error}")
            events.append(event)
            print(format_event(event), flush=True)

        try:
            write_events(spark, run_id, mode, events)
        except Exception as error:
            # Khi chặn, bằng chứng vì sao chặn phải được lưu lại.
            if mode == "block":
                raise
            print(f"WARNING: không ghi được serving.schema_drift_events: {error}")

        code = exit_code(mode, events)
        if code == BLOCKED_EXIT_CODE:
            summary = blocked_summary(events)
            print(f"{BLOCKED_STATUS}: {summary}", flush=True)
            if orchestration_run_id:
                mark_run_blocked(spark, orchestration_run_id, summary)
    finally:
        spark.stop()

    print(json.dumps({
        "run_id": run_id,
        "mode": mode,
        "data_mode": data_mode,
        "actions": {event["source_name"]: event["action"] for event in events},
        "exit_code": code,
    }, ensure_ascii=False))
    return code


if __name__ == "__main__":
    sys.exit(main())
