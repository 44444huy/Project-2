import argparse
import gzip
import json
import sys
from pathlib import Path


# Trường có cấu trúc không đồng nhất giữa các dòng (mỗi product một bộ key khác
# nhau). Chỉ ghi kiểu của trường, không đi vào trong, nếu không mỗi lần chạy sẽ
# báo thêm hoặc xóa hàng loạt cột con.
SOURCE_OPAQUE_FIELDS = {
    "reviews": (),
    "products": ("details",),
}

# Trường không lớp nào dùng (Silver chỉ lưu lại, Gold và dashboard không đọc), nên
# cổng schema bỏ qua hoàn toàn: đổi kiểu hay mất trường cũng không làm hỏng gì.
# Bronze vẫn giữ nguyên JSON gốc, cần dùng lại thì xóa khỏi danh sách này.
SOURCE_IGNORED_FIELDS = {
    "reviews": ("images",),
    "products": (
        "images", "videos", "subtitle", "author", "bought_together", "features",
        "description",
    ),
}

# Hậu tố cho phần tử của mảng: "images[]" là từng phần tử của "images",
# "images[].thumb" là trường thumb bên trong mỗi phần tử.
ELEMENT_SUFFIX = "[]"

# Số chữ số thập phân khi ghi tỷ lệ, để baseline không đổi chỉ vì sai số làm tròn.
RATE_DIGITS = 6


def json_type(value):
    # bool phải xét trước int vì trong Python bool là lớp con của int.
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    raise ValueError(f"Kiểu JSON không hỗ trợ: {type(value).__name__}")


def summarize_types(type_counts):
    # Kiểu của trường là kiểu chiếm đa số trong các giá trị khác null. Các kiểu
    # còn lại là giá trị lỗi (ví dụ price = "—"), chỉ ghi tỷ lệ để người duyệt
    # xem, comparator không so phần này.
    if not type_counts:
        return "null", {}

    counts = dict(type_counts)
    # JSON không phân biệt số nguyên và số thực: 5 và 5.0 đều là số. Gặp cả hai
    # thì coi là number, tránh báo đổi kiểu chỉ vì một dòng ghi 5 thay vì 5.0.
    if "integer" in counts and "number" in counts:
        counts["number"] += counts.pop("integer")

    total = sum(counts.values())
    # Sắp tên trước để khi hai kiểu bằng nhau thì kết quả vẫn cố định.
    main_type = max(sorted(counts), key=counts.get)
    other_types = {
        name: round(count / total, RATE_DIGITS)
        for name, count in sorted(counts.items())
        if name != main_type
    }
    return main_type, other_types


class _Profile:
    def __init__(self, opaque_fields, ignored_fields):
        self.opaque_fields = set(opaque_fields)
        self.ignored_fields = set(ignored_fields)
        self.records = 0
        self.malformed = 0
        # Số lần mỗi object (theo đường dẫn) xuất hiện, làm mẫu số cho tỷ lệ có
        # mặt của các trường con. "" là cấp ngoài cùng, bằng số dòng hợp lệ.
        self.object_counts = {}
        self.fields = {}

    def add_line(self, line):
        line = line.strip()
        if not line:
            return
        try:
            record = json.loads(line)
        except ValueError:
            self.malformed += 1
            return
        if not isinstance(record, dict):
            self.malformed += 1
            return

        self.records += 1
        self._visit_object("", record)

    def _field(self, path, parent):
        stats = self.fields.get(path)
        if stats is None:
            stats = {"parent": parent, "present": 0, "null": 0, "types": {}}
            self.fields[path] = stats
        return stats

    def _visit_object(self, path, value):
        self.object_counts[path] = self.object_counts.get(path, 0) + 1
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else key
            if child_path in self.ignored_fields:
                continue
            self._field(child_path, path)["present"] += 1
            self._visit_value(child_path, child)

    def _visit_value(self, path, value):
        stats = self.fields[path]
        if value is None:
            stats["null"] += 1
            return

        value_type = json_type(value)
        stats["types"][value_type] = stats["types"].get(value_type, 0) + 1
        if path in self.opaque_fields:
            return

        if isinstance(value, dict):
            self._visit_object(path, value)
        elif isinstance(value, list):
            element_path = path + ELEMENT_SUFFIX
            for element in value:
                self._field(element_path, path)["present"] += 1
                self._visit_value(element_path, element)

    def merge(self, other):
        # Gộp số đếm của một profile khác (ví dụ của một phần dữ liệu khác) vào
        # profile này. Hai profile phải dùng cùng danh sách trường opaque và bỏ qua.
        self.records += other.records
        self.malformed += other.malformed
        for path, count in other.object_counts.items():
            self.object_counts[path] = self.object_counts.get(path, 0) + count
        for path, stats in other.fields.items():
            target = self._field(path, stats["parent"])
            target["present"] += stats["present"]
            target["null"] += stats["null"]
            for name, count in stats["types"].items():
                target["types"][name] = target["types"].get(name, 0) + count
        return self

    def result(self):
        return {
            "records": self.records,
            "malformed": self.malformed,
            "schema": self.schema(),
        }

    def schema(self):
        result = {}
        for path, stats in sorted(self.fields.items()):
            if path.endswith(ELEMENT_SUFFIX):
                # Phần tử của mảng luôn "có mặt" khi được đếm; mảng rỗng không
                # sinh phần tử nào.
                denominator = stats["present"]
            else:
                denominator = self.object_counts.get(stats["parent"], 0)

            presence_rate = stats["present"] / denominator if denominator else 0.0
            null_rate = stats["null"] / stats["present"] if stats["present"] else 0.0

            main_type, other_types = summarize_types(stats["types"])
            result[path] = {
                "type": main_type,
                "nullable": stats["null"] > 0 or presence_rate < 1,
                # Báo cho comparator biết nullable là quan sát từ mẫu.
                "observed_nullability": True,
                "logical_type": None,
                "parameters": {},
                # Comparator chỉ so type, nullable, logical_type, parameters; các
                # số liệu dưới đây được giữ trong baseline để so theo ngưỡng sau này.
                "presence_rate": round(presence_rate, RATE_DIGITS),
                "null_rate": round(null_rate, RATE_DIGITS),
                "other_types": other_types,
                "opaque": path in self.opaque_fields,
            }
        return result


def build_profile(lines, opaque_fields=(), ignored_fields=()):
    # Đếm các dòng JSONL thành một profile có thể gộp với profile khác.
    profile = _Profile(opaque_fields, ignored_fields)
    for line in lines:
        profile.add_line(line)
    return profile


def profile_lines(lines, opaque_fields=(), ignored_fields=()):
    # Suy ra schema từ các dòng JSONL. Trả về schema theo định dạng comparator
    # dùng, kèm số dòng hợp lệ và số dòng không phải JSON object.
    return build_profile(lines, opaque_fields, ignored_fields).result()


def read_lines(path, limit=None, step=1):
    # Đọc file .jsonl hoặc .jsonl.gz, lấy mỗi `step` dòng một dòng, tối đa
    # `limit` dòng. step > 1 giúp mẫu trải đều hơn trong một phần dữ liệu.
    if step < 1:
        raise ValueError("step phải lớn hơn hoặc bằng 1")
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open

    taken = 0
    with opener(path, "rt", encoding="utf-8") as file:
        for index, line in enumerate(file):
            if limit is not None and taken >= limit:
                return
            if index % step == 0:
                taken += 1
                yield line


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=sorted(SOURCE_OPAQUE_FIELDS))
    parser.add_argument("path")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--step", type=int, default=1)
    args = parser.parse_args()

    # Console Windows mặc định cp1252, không in được lý do tiếng Việt.
    sys.stdout.reconfigure(encoding="utf-8")

    result = profile_lines(
        read_lines(args.path, args.limit, args.step),
        SOURCE_OPAQUE_FIELDS[args.source],
        SOURCE_IGNORED_FIELDS[args.source],
    )
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
