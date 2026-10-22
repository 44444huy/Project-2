import json


ADD_FIELD = "ADD_FIELD"
REMOVE_FIELD = "REMOVE_FIELD"
TYPE_CHANGE = "TYPE_CHANGE"
NULLABILITY_CHANGE = "NULLABILITY_CHANGE"
PARAMETER_CHANGE = "PARAMETER_CHANGE"


def describe_type(column):
    # Gộp type vật lý + logical type thành một chuỗi để so sánh,
    # ví dụ bytes + Decimal(12, 2) -> "Decimal(12,2)".
    logical_type = column.get("logical_type")

    if not logical_type:
        return column["type"]

    short_name = logical_type.rsplit(".", 1)[-1]
    parameters = column.get("parameters", {})

    if short_name == "Decimal":
        precision = parameters.get("connect.decimal.precision")
        scale = parameters.get("scale")
        return f"Decimal({precision},{scale})"

    return short_name


def compare_schemas(old_schema, new_schema):
    changes = []

    for field in sorted(new_schema.keys() - old_schema.keys()):
        column = new_schema[field]
        changes.append({
            "field": field,
            "change": ADD_FIELD,
            "type": describe_type(column),
            "nullable": column["nullable"],
        })

    for field in sorted(old_schema.keys() - new_schema.keys()):
        column = old_schema[field]
        changes.append({
            "field": field,
            "change": REMOVE_FIELD,
            "type": describe_type(column),
            "nullable": column["nullable"],
        })

    for field in sorted(old_schema.keys() & new_schema.keys()):
        old_column = old_schema[field]
        new_column = new_schema[field]

        old_type = describe_type(old_column)
        new_type = describe_type(new_column)

        if old_type != new_type:
            changes.append({
                "field": field,
                "change": TYPE_CHANGE,
                "from": old_type,
                "to": new_type,
                "nullable": new_column["nullable"],
            })
        else:
            # Cùng kiểu nhưng tham số khác, ví dụ Enum đổi danh sách
            # "allowed" hoặc cột đổi độ dài. Decimal đã được describe_type
            # xử lý nên không rơi vào nhánh này.
            old_parameters = old_column.get("parameters") or {}
            new_parameters = new_column.get("parameters") or {}

            if old_parameters != new_parameters:
                changes.append({
                    "field": field,
                    "change": PARAMETER_CHANGE,
                    "type": new_type,
                    "from": old_parameters,
                    "to": new_parameters,
                })

        if old_column["nullable"] != new_column["nullable"]:
            changes.append({
                "field": field,
                "change": NULLABILITY_CHANGE,
                "from": old_column["nullable"],
                "to": new_column["nullable"],
            })

    _mark_rename_candidates(changes, old_schema, new_schema)
    return changes


def _signature(column):
    # Hai cột "giống nhau" khi cùng kiểu, nullability và tham số.
    return (
        describe_type(column),
        column["nullable"],
        json.dumps(column.get("parameters") or {}, sort_keys=True),
    )


def _mark_rename_candidates(changes, old_schema, new_schema):
    # Chỉ gợi ý, không quyết định: một cột bị xóa và một cột mới có cùng chữ ký,
    # ghép được một-một duy nhất, thì đánh dấu là ứng viên đổi tên để người xác nhận.
    removed = {}
    added = {}

    for change in changes:
        if change["change"] == REMOVE_FIELD:
            removed.setdefault(_signature(old_schema[change["field"]]), []).append(change)
        elif change["change"] == ADD_FIELD:
            added.setdefault(_signature(new_schema[change["field"]]), []).append(change)

    for signature, removed_changes in removed.items():
        added_changes = added.get(signature, [])
        if len(removed_changes) == 1 and len(added_changes) == 1:
            removed_changes[0]["rename_candidate"] = added_changes[0]["field"]
            added_changes[0]["renamed_from"] = removed_changes[0]["field"]
