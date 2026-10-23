import itertools
import json
import os

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


# report: chỉ phát hiện và ghi lại, luôn để pipeline chạy tiếp. Chế độ chặn sẽ
# được thêm sau khi report chạy ổn trên dữ liệu thật.
SUPPORTED_MODES = ("report",)
DEFAULT_MODE = "report"
DEFAULT_LINES_PER_PART = 20000
# Baseline đã duyệt nằm trong Git (schemas/<nguồn>/vN.json), mount chỉ đọc.
DEFAULT_BASELINE_ROOT = "/opt/project/schemas"
SOURCES = ("reviews", "products")
ERROR = "ERROR"

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


def main():
    mode = configured_mode()
    lines_per_partition = configured_lines_per_part()
    data_mode = configured_data_mode()
    run_id = os.environ.get("ORCHESTRATION_RUN_ID") or "manual-schema-check"
    baseline_root = os.environ.get("SCHEMA_BASELINE_ROOT", DEFAULT_BASELINE_ROOT)
    dataset, sources = load_dataset_config()

    spark = SparkSession.builder.appName("check-raw-schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    events = []
    try:
        for name in SOURCES:
            # Chế độ report không được làm dừng pipeline: lỗi của chính bước kiểm
            # tra cũng chỉ được ghi lại.
            try:
                event = check_source(
                    spark, dataset, sources[name], data_mode,
                    lines_per_partition, baseline_root,
                )
            except Exception as error:
                event = error_event(name, f"{type(error).__name__}: {error}")
            events.append(event)
            print(format_event(event), flush=True)

        try:
            write_events(spark, run_id, mode, events)
        except Exception as error:
            print(f"WARNING: không ghi được serving.schema_drift_events: {error}")
    finally:
        spark.stop()

    print(json.dumps({
        "run_id": run_id,
        "mode": mode,
        "data_mode": data_mode,
        "actions": {event["source_name"]: event["action"] for event in events},
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
