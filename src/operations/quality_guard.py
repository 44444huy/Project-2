import json
import os
import sys

from pyspark.sql import SparkSession


GOLD_QUALITY_TABLE = "polaris.gold.data_quality_summary"
SUPPORTED_MODES = ("report", "block")
BLOCKED_EXIT_CODE = 99
BLOCKED_STATUS = "QUALITY_GUARD_BLOCKED"
PASS = "PASS"
BLOCK = "BLOCK"
OVERRIDDEN = "OVERRIDDEN"

# Increases are absolute rate differences, so a source whose previous reject
# rate was zero still has a meaningful limit.
DEFAULT_THRESHOLDS = {
    "max_reject_rate_increase": 0.01,
    "max_issue_rate_increase": 0.05,
    "max_accepted_drop": 0.05,
}
THRESHOLD_ENVIRONMENT = {
    "max_reject_rate_increase": "QUALITY_GUARD_MAX_REJECT_RATE_INCREASE",
    "max_issue_rate_increase": "QUALITY_GUARD_MAX_ISSUE_RATE_INCREASE",
    "max_accepted_drop": "QUALITY_GUARD_MAX_ACCEPTED_DROP",
}

PREVIOUS_QUALITY_SQL = """
    SELECT source_name, metric_type, metric_name, row_count
    FROM serving.data_quality_summary
"""
PUBLISHED_RUN_SQL = """
    SELECT run_id, data_mode
    FROM serving.pipeline_runs
    WHERE published_at IS NOT NULL
    ORDER BY published_at DESC
    LIMIT 1
"""
UPSERT_CHECK_SQL = """
    INSERT INTO serving.quality_guard_checks (
        run_id, check_name, source_name, metric_name, mode, action,
        previous_value, current_value, limit_value, message, checked_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
    ON CONFLICT (run_id, check_name, source_name, metric_name) DO UPDATE SET
        mode = EXCLUDED.mode,
        action = EXCLUDED.action,
        previous_value = EXCLUDED.previous_value,
        current_value = EXCLUDED.current_value,
        limit_value = EXCLUDED.limit_value,
        message = EXCLUDED.message,
        checked_at = NOW()
"""
MARK_BLOCKED_SQL = """
    UPDATE serving.pipeline_runs
    SET status = 'QUALITY_GUARD_BLOCKED',
        finished_at = NOW(),
        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
        error_message = ?,
        updated_at = NOW()
    WHERE run_id = ?
"""


def configured_mode(value=None):
    mode = (value or os.environ.get("QUALITY_GUARD_MODE", "report")).strip().lower()
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"QUALITY_GUARD_MODE must be one of {list(SUPPORTED_MODES)}")
    return mode


def configured_thresholds(environment=None):
    environment = os.environ if environment is None else environment
    thresholds = dict(DEFAULT_THRESHOLDS)
    for name, variable in THRESHOLD_ENVIRONMENT.items():
        if environment.get(variable):
            value = float(environment[variable])
            if value < 0:
                raise ValueError(f"{variable} must not be negative")
            thresholds[name] = value
    return thresholds


def override_requested(value=None):
    # Rendered from dag_run.conf["allow_quality_change"]; Jinja gives "True".
    value = os.environ.get("QUALITY_GUARD_OVERRIDE", "") if value is None else value
    return str(value).strip().lower() in {"1", "true", "yes"}


def summarize(rows):
    summary = {}
    for row in rows:
        source = summary.setdefault(
            row["source_name"],
            {"total": 0, "accepted": 0, "rejected": 0, "issues": {}},
        )
        count = int(row["row_count"])
        if row["metric_type"] == "STATUS":
            source["total"] += count
            if row["metric_name"] == "REJECTED":
                source["rejected"] += count
            else:
                source["accepted"] += count
        elif row["metric_type"] == "ISSUE":
            source["issues"][row["metric_name"]] = count
    return summary


def rate(count, total):
    return count / total if total else 0.0


def check(name, source, metric, previous, current, limit, failed, message):
    return {
        "check_name": name,
        "source_name": source,
        "metric_name": metric,
        "previous_value": previous,
        "current_value": current,
        "limit_value": limit,
        "action": BLOCK if failed else PASS,
        "message": message,
    }


def evaluate_quality(previous_rows, current_rows, thresholds, compare_counts=True):
    previous = summarize(previous_rows)
    current = summarize(current_rows)
    if not previous:
        return [check(
            "BASELINE", "*", "*", None, None, None, False,
            "No published quality summary yet; this run becomes the reference.",
        )]

    checks = []
    for source_name in sorted(set(previous) | set(current)):
        before = previous.get(source_name)
        after = current.get(source_name)
        if after is None or after["total"] == 0:
            checks.append(check(
                "SOURCE_PRESENT", source_name, "*", None, 0, None, True,
                f"{source_name} has no quality rows in this run.",
            ))
            continue
        if before is None:
            checks.append(check(
                "SOURCE_PRESENT", source_name, "*", None, after["total"], None, False,
                f"{source_name} is new compared with the published version.",
            ))
            continue

        limit = thresholds["max_reject_rate_increase"]
        old_rate = rate(before["rejected"], before["total"])
        new_rate = rate(after["rejected"], after["total"])
        checks.append(check(
            "REJECT_RATE", source_name, "REJECTED", old_rate, new_rate, limit,
            new_rate - old_rate > limit,
            f"Reject rate {old_rate:.4%} -> {new_rate:.4%}.",
        ))

        limit = thresholds["max_issue_rate_increase"]
        for issue in sorted(set(before["issues"]) | set(after["issues"])):
            old_rate = rate(before["issues"].get(issue, 0), before["total"])
            new_rate = rate(after["issues"].get(issue, 0), after["total"])
            checks.append(check(
                "ISSUE_RATE", source_name, issue, old_rate, new_rate, limit,
                new_rate - old_rate > limit,
                f"{issue} rate {old_rate:.4%} -> {new_rate:.4%}.",
            ))

        if compare_counts:
            limit = thresholds["max_accepted_drop"]
            drop = rate(before["accepted"] - after["accepted"], before["accepted"])
            checks.append(check(
                "ACCEPTED_ROWS", source_name, "ACCEPTED",
                before["accepted"], after["accepted"], limit, drop > limit,
                f"Accepted rows {before['accepted']:,} -> {after['accepted']:,}.",
            ))
    return checks


def apply_override(checks, override):
    if not override:
        return checks
    return [
        {**item, "action": OVERRIDDEN} if item["action"] == BLOCK else item
        for item in checks
    ]


def exit_code(mode, checks):
    if mode == "block" and any(item["action"] == BLOCK for item in checks):
        return BLOCKED_EXIT_CODE
    return 0


def blocked_summary(checks):
    held = [item for item in checks if item["action"] == BLOCK]
    return "Quality changed beyond the guard limits; " + "; ".join(
        f"{item['source_name']} {item['check_name']} {item['metric_name']}: "
        f"{item['message']}"
        for item in held
    )


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


def query_rows(connection, sql, columns):
    statement = connection.createStatement()
    try:
        result = statement.executeQuery(sql)
        rows = []
        while result.next():
            rows.append({name: result.getString(name) for name in columns})
        result.close()
        return rows
    finally:
        statement.close()


def write_checks(connection, sql_types, run_id, mode, checks):
    statement = connection.prepareStatement(UPSERT_CHECK_SQL)
    try:
        for item in checks:
            values = [
                (run_id, "VARCHAR"),
                (item["check_name"], "VARCHAR"),
                (item["source_name"], "VARCHAR"),
                (item["metric_name"], "VARCHAR"),
                (mode, "VARCHAR"),
                (item["action"], "VARCHAR"),
                (item["previous_value"], "DOUBLE"),
                (item["current_value"], "DOUBLE"),
                (item["limit_value"], "DOUBLE"),
                (item["message"], "VARCHAR"),
            ]
            for index, (value, sql_type) in enumerate(values, start=1):
                if value is None:
                    statement.setNull(index, getattr(sql_types, sql_type))
                elif sql_type == "DOUBLE":
                    statement.setDouble(index, float(value))
                else:
                    statement.setString(index, str(value))
            statement.executeUpdate()
    finally:
        statement.close()


def mark_run_blocked(connection, run_id, message):
    statement = connection.prepareStatement(MARK_BLOCKED_SQL)
    try:
        statement.setString(1, message[:4000])
        statement.setString(2, run_id)
        if statement.executeUpdate() != 1:
            raise RuntimeError(f"No pipeline run row found for {run_id}")
    finally:
        statement.close()


def main():
    mode = configured_mode()
    try:
        return run_guard(mode)
    except Exception as error:
        # Report mode must never stop the pipeline, even when the guard itself
        # fails. Block mode lets the task fail so Airflow can retry it.
        if mode == "block":
            raise
        print(f"WARNING: quality guard could not run: {type(error).__name__}: {error}")
        return 0


def run_guard(mode):
    thresholds = configured_thresholds()
    override = override_requested()
    data_mode = os.environ.get("DATA_MODE", "million").strip().lower()
    orchestration_run_id = os.environ.get("ORCHESTRATION_RUN_ID")
    run_id = orchestration_run_id or "manual-quality-guard"

    spark = SparkSession.builder.appName("quality-guard").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    connection = None
    try:
        current_rows = [row.asDict() for row in spark.table(GOLD_QUALITY_TABLE).collect()]
        connection = open_postgres_connection(spark)
        previous_rows = query_rows(
            connection, PREVIOUS_QUALITY_SQL,
            ("source_name", "metric_type", "metric_name", "row_count"),
        )
        published = query_rows(connection, PUBLISHED_RUN_SQL, ("run_id", "data_mode"))
        # Row counts are only comparable within the same processing scope.
        compare_counts = bool(published) and published[0]["data_mode"] == data_mode

        checks = apply_override(
            evaluate_quality(previous_rows, current_rows, thresholds, compare_counts),
            override,
        )
        for item in checks:
            if item["action"] != PASS:
                print(f"{item['action']}: {item['source_name']} {item['check_name']} "
                      f"{item['metric_name']}: {item['message']}", flush=True)

        write_checks(connection, spark._jvm.java.sql.Types, run_id, mode, checks)
        code = exit_code(mode, checks)
        if code == BLOCKED_EXIT_CODE:
            summary = blocked_summary(checks)
            print(f"{BLOCKED_STATUS}: {summary}", flush=True)
            if orchestration_run_id:
                mark_run_blocked(connection, orchestration_run_id, summary)
    finally:
        if connection is not None:
            connection.close()
        spark.stop()

    print(json.dumps({
        "run_id": run_id,
        "mode": mode,
        "override": override,
        "compared_counts": compare_counts,
        "blocked": sum(item["action"] == BLOCK for item in checks),
        "overridden": sum(item["action"] == OVERRIDDEN for item in checks),
        "checks": len(checks),
        "exit_code": code,
    }))
    return code


if __name__ == "__main__":
    sys.exit(main())
