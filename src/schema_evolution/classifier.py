import re

from schema_evolution.comparator import (
    ADD_FIELD,
    NULLABILITY_CHANGE,
    PARAMETER_CHANGE,
    REMOVE_FIELD,
    TYPE_CHANGE,
)
from schema_evolution.dependencies import uses_column


INFO = "INFO"
WARNING = "WARNING"
BREAKING = "BREAKING"
CRITICAL = "CRITICAL"

# Thứ tự tăng dần theo mức độ nguy hiểm, dùng để lấy mức tệ nhất.
SEVERITY_ORDER = [INFO, WARNING, BREAKING, CRITICAL]

# Kiểu số của Kafka Connect: chuyển sang kiểu rộng hơn không mất dữ liệu.
WIDENING = {
    ("int8", "int16"),
    ("int8", "int32"),
    ("int8", "int64"),
    ("int16", "int32"),
    ("int16", "int64"),
    ("int32", "int64"),
    ("float32", "float64"),
}

DECIMAL_PATTERN = re.compile(r"^Decimal\((\d+),(\d+)\)$")


def worst_severity(severities):
    return max(severities, key=SEVERITY_ORDER.index, default=INFO)


def classify_change(change, dependencies=None):
    severity, reason = _classify_base(change, dependencies)

    # Ứng viên đổi tên luôn bị chặn để người xác nhận, kể cả khi cột cũ không ai
    # dùng hoặc cột mới là nullable, vì đổi tên làm đứt mạch dữ liệu theo tên cột.
    if change.get("rename_candidate"):
        reason = (
            f"Ứng viên đổi tên thành {change['rename_candidate']}, "
            f"cần xác nhận thủ công. {reason}"
        )
        severity = worst_severity([severity, BREAKING])
    elif change.get("renamed_from"):
        reason = (
            f"Ứng viên đổi tên từ {change['renamed_from']}, "
            f"cần xác nhận thủ công. {reason}"
        )
        severity = worst_severity([severity, BREAKING])

    return severity, reason


def _classify_base(change, dependencies=None):
    # Trả về (severity, reason). Loại thay đổi không biết thì coi là BREAKING
    # để fail an toàn thay vì cho qua.
    kind = change["change"]

    if kind == ADD_FIELD:
        if change["nullable"]:
            return INFO, "Thêm cột nullable, consumer cũ không bị ảnh hưởng"
        return WARNING, "Thêm cột bắt buộc, cần kiểm tra consumer và dữ liệu cũ"

    if kind == REMOVE_FIELD:
        return _classify_removal(change, dependencies)

    if kind == NULLABILITY_CHANGE:
        if change["to"]:
            return WARNING, "Cột chuyển từ NOT NULL sang nullable, logic downstream có thể sai"
        return BREAKING, "Cột chuyển từ nullable sang NOT NULL, dữ liệu cũ có thể vi phạm"

    if kind == TYPE_CHANGE:
        return _classify_type_change(change)

    if kind == PARAMETER_CHANGE:
        return _classify_parameter_change(change)

    return BREAKING, f"Loại thay đổi chưa được hỗ trợ: {kind}"


def _classify_removal(change, dependencies):
    # Không biết lớp nào dùng cột thì coi là breaking để an toàn.
    if dependencies is None:
        return BREAKING, "Xóa cột, consumer còn đọc cột này sẽ hỏng"

    field = change["field"]

    if uses_column(dependencies.get("gold", []), field):
        return CRITICAL, f"Xóa cột {field} đang được Gold dùng, ảnh hưởng báo cáo"
    if uses_column(dependencies.get("silver", []), field):
        return BREAKING, f"Xóa cột {field} đang được Silver dùng"

    return WARNING, (
        f"Xóa cột {field} không lớp nào khai báo dùng, "
        "kiểm tra file dependency còn đúng không"
    )


def _classify_type_change(change):
    old_type, new_type = change["from"], change["to"]

    if (old_type, new_type) in WIDENING:
        return INFO, f"{old_type} -> {new_type} mở rộng kiểu, không mất dữ liệu"

    old_decimal = DECIMAL_PATTERN.match(old_type)
    new_decimal = DECIMAL_PATTERN.match(new_type)
    if old_decimal and new_decimal:
        old_precision, old_scale = map(int, old_decimal.groups())
        new_precision, new_scale = map(int, new_decimal.groups())
        # An toàn khi cả phần nguyên lẫn phần thập phân đều không bị thu hẹp.
        if (new_scale >= old_scale
                and new_precision - new_scale >= old_precision - old_scale):
            return INFO, f"{old_type} -> {new_type} mở rộng Decimal, không mất dữ liệu"
        return BREAKING, f"{old_type} -> {new_type} thu hẹp Decimal, có thể mất dữ liệu"

    return BREAKING, f"Đổi kiểu {old_type} -> {new_type}, không nằm trong danh sách an toàn"


def _classify_parameter_change(change):
    old_parameters, new_parameters = change["from"], change["to"]
    changed_keys = sorted(
        key for key in old_parameters.keys() | new_parameters.keys()
        if old_parameters.get(key) != new_parameters.get(key)
    )

    results = [
        _classify_parameter(key, old_parameters.get(key), new_parameters.get(key))
        for key in changed_keys
    ]
    severity = worst_severity(result[0] for result in results)
    reason = "; ".join(result[1] for result in results if result[0] == severity)
    return severity, reason


def _classify_parameter(key, old_value, new_value):
    if key == "allowed":
        old_values = set((old_value or "").split(","))
        new_values = set((new_value or "").split(","))
        removed = old_values - new_values
        if removed:
            return BREAKING, f"Enum bị bớt giá trị: {', '.join(sorted(removed))}"
        added = new_values - old_values
        return WARNING, f"Enum thêm giá trị: {', '.join(sorted(added))}"

    if key == "length":
        if old_value is not None and new_value is not None:
            try:
                if int(new_value) >= int(old_value):
                    return INFO, f"Tăng độ dài cột {old_value} -> {new_value}"
                return BREAKING, f"Giảm độ dài cột {old_value} -> {new_value}"
            except ValueError:
                pass
        return WARNING, f"Độ dài cột đổi {old_value} -> {new_value}"

    return WARNING, f"Tham số {key} đổi {old_value} -> {new_value}"


def classify_changes(changes, dependencies=None):
    classified = []

    for change in changes:
        severity, reason = classify_change(change, dependencies)
        classified.append({**change, "severity": severity, "reason": reason})

    return classified
