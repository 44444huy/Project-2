"""Rejoin a JSONL review split across two Raw gzip parts by an interrupted download.

Works only on local copies. It never talks to MinIO: download the two parts and
manifest.json first, run this script, review its output, then upload the
corrected files (see docs/raw_repair_split_line.md).
"""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True,
                        help="directory with manifest.json and both parts")
    parser.add_argument("--source", default="reviews")
    parser.add_argument("--first", default="part-00032.jsonl.gz")
    parser.add_argument("--second", default="part-00033.jsonl.gz")
    return parser.parse_args()


def find_part(source_manifest, file_name):
    for part in source_manifest["parts"]:
        if part["file_name"] == file_name:
            return part
    raise ValueError(f"{file_name} is not in the manifest")


def write_gzip(path, chunks):
    # compresslevel=1 matches scripts/download_full_dataset.py.
    with gzip.open(path, "wb", compresslevel=1) as stream:
        for chunk in chunks:
            stream.write(chunk)


def repair(workdir, source_name, first_name, second_name):
    manifest_path = workdir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source = manifest["sources"][source_name]
    first_part = find_part(source, first_name)
    second_part = find_part(source, second_name)

    first_path = workdir / first_name
    second_path = workdir / second_name
    for path, part in ((first_path, first_part), (second_path, second_part)):
        actual = sha256_file(path)
        if actual != part["sha256"]:
            raise RuntimeError(f"{path.name}: sha256 {actual} != manifest {part['sha256']}")

    first_bytes = gzip.open(first_path, "rb").read()
    if first_bytes.endswith(b"\n"):
        raise RuntimeError(f"{first_name} already ends with a newline; nothing to repair")
    split_at = first_bytes.rfind(b"\n") + 1
    first_complete, tail = first_bytes[:split_at], first_bytes[split_at:]

    with gzip.open(second_path, "rb") as stream:
        head = stream.readline()
        second_rest = stream.read()
    if not head.endswith(b"\n") or not second_rest.endswith(b"\n"):
        raise RuntimeError(f"{second_name}: expected complete newline-terminated lines")
    joined = tail + head
    review = json.loads(joined)  # raises if the halves do not form one record
    for halve in (tail, head):
        try:
            json.loads(halve)
        except json.JSONDecodeError:
            continue
        raise RuntimeError("a split half parses on its own; refusing to join")

    repaired_dir = workdir / "repaired"
    repaired_dir.mkdir(exist_ok=True)
    new_first = repaired_dir / first_name
    new_second = repaired_dir / second_name
    write_gzip(new_first, [first_complete])
    write_gzip(new_second, [joined, second_rest])

    # Verify the repaired pair restores exactly the original source bytes.
    restored = gzip.open(new_first, "rb").read() + gzip.open(new_second, "rb").read()
    original_second = gzip.open(second_path, "rb").read()
    if restored != first_bytes + original_second:
        raise RuntimeError("repaired parts do not restore the original bytes")

    old_first_lines = first_part["line_count"]
    first_part.update(
        line_count=first_complete.count(b"\n"),
        uncompressed_bytes=len(first_complete),
        compressed_bytes=new_first.stat().st_size,
        sha256=sha256_file(new_first),
    )
    second_part.update(
        line_count=1 + second_rest.count(b"\n"),
        uncompressed_bytes=len(joined) + len(second_rest),
        compressed_bytes=new_second.stat().st_size,
        sha256=sha256_file(new_second),
    )
    source["line_count"] = sum(part["line_count"] for part in source["parts"])
    source["compressed_bytes"] = sum(part["compressed_bytes"] for part in source["parts"])
    if sum(part["uncompressed_bytes"] for part in source["parts"]) != source["source_bytes"]:
        raise RuntimeError("part byte sizes no longer add up to source_bytes")

    new_manifest = repaired_dir / "manifest.json"
    temporary = new_manifest.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")
    os.replace(temporary, new_manifest)

    print(f"moved {len(tail)} bytes from the end of {first_name} to {second_name}")
    print(f"rejoined review: user_id={review.get('user_id')} "
          f"parent_asin={review.get('parent_asin')} timestamp={review.get('timestamp')}")
    print(f"{first_name}: {old_first_lines} -> {first_part['line_count']} lines; "
          f"sha256 {first_part['sha256']}")
    print(f"{second_name}: {second_part['line_count']} lines; sha256 {second_part['sha256']}")
    print(f"{source_name} line_count: {source['line_count']}")
    print(f"wrote {repaired_dir}")


def main():
    args = parse_args()
    repair(args.workdir, args.source, args.first, args.second)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
