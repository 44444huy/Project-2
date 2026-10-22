import json
from pathlib import Path


# bronze chỉ mang tính ghi chú (Bronze giữ raw nên xóa cột không làm hỏng nó);
# classifier chỉ dùng silver và gold để đánh giá việc xóa cột.
LAYERS = ("bronze", "silver", "gold")


def load_dependencies(path):
    # Trả None nếu bảng chưa khai báo dependency; file sai định dạng thì báo lỗi
    # to thay vì lặng lẽ bỏ qua, vì cấu hình sai dễ cho qua nhầm cột đang dùng.
    path = Path(path)
    if not path.is_file():
        return None

    try:
        document = json.loads(path.read_text(encoding="utf-8-sig"))
    except ValueError as error:
        raise ValueError(f"{path}: không phải JSON hợp lệ ({error})") from error

    if not isinstance(document, dict):
        raise ValueError(f"{path}: phải là một JSON object")

    unknown = set(document) - set(LAYERS)
    if unknown:
        raise ValueError(f"{path}: lớp không hợp lệ {sorted(unknown)}, chỉ nhận {list(LAYERS)}")

    for layer, columns in document.items():
        if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
            raise ValueError(f"{path}: '{layer}' phải là danh sách tên cột")

    return document


def uses_column(columns, field):
    # "*" là dùng mọi cột. Cột lồng nhau khớp theo tiền tố: khai báo "address"
    # bao cả "address.city", và xóa "address" làm mất cả "address.city".
    for column in columns:
        if column == "*" or column == field:
            return True
        if field.startswith(column + ".") or column.startswith(field + "."):
            return True
    return False
