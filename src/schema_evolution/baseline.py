import argparse
import copy
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from schema_evolution.comparator import (
    ADD_FIELD,
    NULLABILITY_CHANGE,
    PARAMETER_CHANGE,
    REMOVE_FIELD,
    TYPE_CHANGE,
    compare_schemas,
)
from schema_evolution.dependencies import load_dependencies
from schema_evolution.policy import ACCEPT, decide


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Trạng thái chạy do máy tự ghi (ACCEPT, approve). Nằm trong .gitignore,
# không commit.
DEFAULT_ROOT = PROJECT_ROOT / "schema_baselines"

# Bản sao các phiên bản do người duyệt (approve) để commit và review qua PR.
REVIEW_ROOT = PROJECT_ROOT / "schemas"

VERSION_FILE_PATTERN = re.compile(r"^v(\d+)\.json$")
# Tên bảng dùng làm tên thư mục, chặn "../" và ký tự lạ.
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def _table_dir(table, root):
    if not TABLE_NAME_PATTERN.match(table) or table in {".", ".."}:
        raise ValueError(f"Tên bảng không hợp lệ: {table!r}")
    return Path(root) / table


def list_versions(table, root=DEFAULT_ROOT):
    table_dir = _table_dir(table, root)
    if not table_dir.is_dir():
        return []

    versions = []
    for path in table_dir.iterdir():
        match = VERSION_FILE_PATTERN.match(path.name)
        if match:
            versions.append(int(match.group(1)))

    # Sắp theo số, không theo chuỗi, để v10 đứng sau v9.
    return sorted(versions)


def load_baseline(table, root=DEFAULT_ROOT):
    # Baseline hiện hành là phiên bản lớn nhất. Chưa có thì trả None.
    versions = list_versions(table, root)
    if not versions:
        return None

    path = _table_dir(table, root) / f"v{versions[-1]}.json"
    return json.loads(path.read_text(encoding="utf-8-sig"))


def save_version(table, schema, reason, root=DEFAULT_ROOT):
    versions = list_versions(table, root)
    version = versions[-1] + 1 if versions else 1

    document = {
        "table": table,
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "reason": reason,
        "schema": schema,
    }

    _write_document(table, document, root)
    return document


def _write_document(table, document, root):
    table_dir = _table_dir(table, root)
    table_dir.mkdir(parents=True, exist_ok=True)

    # Mode "x" không bao giờ ghi đè phiên bản đã có.
    with open(table_dir / f"v{document['version']}.json", "x", encoding="utf-8") as file:
        json.dump(document, file, indent=2, ensure_ascii=False, sort_keys=True)
        file.write("\n")


def load_schema(path):
    # File schema dạng {tên_trường: {type, nullable, logical_type, parameters}}.
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _describe(changes):
    return ", ".join(f"{change['change']} {change['field']}" for change in changes)


# Khóa của một cột mà từng loại thay đổi được phép cập nhật khi tự động ACCEPT.
CHANGE_KEYS = {
    TYPE_CHANGE: ("type", "logical_type", "parameters"),
    PARAMETER_CHANGE: ("parameters",),
    NULLABILITY_CHANGE: ("nullable",),
}


def apply_accepted_changes(old_schema, new_schema, changes):
    # Phiên bản mới = baseline cũ + đúng các thay đổi đã được ACCEPT. Không thay cả
    # schema bằng schema quan sát được, để giữ quyết định đã duyệt (ví dụ trường
    # khai báo nullable dù mẫu chưa có null, hay kiểu lấy theo tài liệu) và các ghi
    # chú như description, source_note.
    merged = copy.deepcopy(old_schema)
    for change in changes:
        field = change["field"]
        if change["change"] == ADD_FIELD:
            merged[field] = copy.deepcopy(new_schema[field])
            # Giữ đúng nullable mà comparator đã đánh giá (trường mới quan sát
            # được từ JSON là tùy chọn cho đến khi được duyệt).
            merged[field]["nullable"] = change["nullable"]
        elif change["change"] == REMOVE_FIELD:
            merged.pop(field, None)
        elif change["change"] in CHANGE_KEYS:
            for key in CHANGE_KEYS[change["change"]]:
                merged[field][key] = copy.deepcopy(new_schema[field].get(key))
        else:
            # Loại thay đổi chưa biết: lấy nguyên cột mới cho đúng với schema đã so.
            merged[field] = copy.deepcopy(new_schema[field])
    return merged


def evaluate(table, new_schema, root=DEFAULT_ROOT, severity_actions=None,
             dependencies_root=None, apply=True):
    # So schema mới với baseline. Chỉ ACCEPT mới tự nâng phiên bản;
    # REVIEW và BLOCK giữ baseline cũ để event sau vẫn bị so với schema đã duyệt.
    # apply=False chỉ đánh giá, không ghi baseline (dùng cho event đang phải chờ
    # vì đứng sau event bị giữ, để baseline không tiến lên sai thứ tự).
    baseline = load_baseline(table, root)

    if baseline is None and not apply:
        return {"action": ACCEPT, "changes": [], "baseline_version": None,
                "new_version": None}

    if baseline is None:
        # Chưa có gì để so: schema đầu tiên thấy được làm baseline v1.
        document = save_version(table, new_schema, "Baseline đầu tiên", root)
        return {
            "action": ACCEPT,
            "changes": [],
            "baseline_version": document["version"],
            "new_version": document["version"],
        }

    dependencies = None
    if dependencies_root is not None:
        dependencies = load_dependencies(
            _table_dir(table, dependencies_root) / "dependencies.json"
        )

    decision = decide(
        compare_schemas(baseline["schema"], new_schema),
        severity_actions,
        dependencies,
    )

    new_version = None
    if apply and decision["action"] == ACCEPT and decision["changes"]:
        document = save_version(
            table,
            apply_accepted_changes(baseline["schema"], new_schema, decision["changes"]),
            f"Tự động ACCEPT: {_describe(decision['changes'])}",
            root,
        )
        new_version = document["version"]

    return {
        **decision,
        "baseline_version": new_version or baseline["version"],
        "new_version": new_version,
    }


def approve(table, new_schema, reason, root=DEFAULT_ROOT, review_root=None):
    # Người duyệt chấp nhận thay đổi bị REVIEW hoặc BLOCK: nâng phiên bản bằng tay.
    # Có review_root thì ghi thêm một bản sao ở đó để commit và review qua PR.
    if not reason.strip():
        raise ValueError("Cần ghi lý do khi duyệt thay đổi")

    document = save_version(table, new_schema, f"Duyệt thủ công: {reason}", root)

    if review_root is not None:
        _write_document(table, document, review_root)

    return document


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("table")
    check_parser.add_argument("schema_file")
    check_parser.add_argument("--dependencies-root", default=str(REVIEW_ROOT))

    approve_parser = subparsers.add_parser("approve")
    approve_parser.add_argument("table")
    approve_parser.add_argument("schema_file")
    approve_parser.add_argument("--reason", required=True)
    approve_parser.add_argument("--review-root", default=str(REVIEW_ROOT))

    args = parser.parse_args()

    # Console Windows mặc định cp1252, không in được lý do tiếng Việt.
    sys.stdout.reconfigure(encoding="utf-8")

    schema = load_schema(args.schema_file)

    if args.command == "check":
        result = evaluate(
            args.table, schema, args.root, dependencies_root=args.dependencies_root
        )
    else:
        document = approve(
            args.table, schema, args.reason, args.root, args.review_root
        )
        result = {
            "approved_version": document["version"],
            "reason": document["reason"],
            "review_copy": str(Path(args.review_root) / args.table),
        }

    print(json.dumps(result, indent=2, ensure_ascii=False))
