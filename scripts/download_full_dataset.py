import argparse
import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_CHUNK_MIB = 512
USER_AGENT = "Project2-full-dataset/1.0"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Stream the complete configured JSONL sources into resumable, "
            "parallel-readable gzip parts."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="output directory (default: <project>/data/full)",
    )
    parser.add_argument(
        "--chunk-mib",
        type=int,
        default=DEFAULT_CHUNK_MIB,
        help="uncompressed MiB per gzip part (default: 512)",
    )
    parser.add_argument(
        "--max-mib-per-second",
        type=float,
        default=0,
        help="maximum source download speed in MiB/s; 0 means unlimited",
    )
    return parser.parse_args()


class BandwidthLimiter:
    def __init__(self, max_bytes_per_second):
        self.max_bytes_per_second = max_bytes_per_second
        self.started_at = time.monotonic()
        self.bytes_received = 0

    def consume(self, byte_count):
        if self.max_bytes_per_second <= 0:
            return
        self.bytes_received += byte_count
        expected_elapsed = self.bytes_received / self.max_bytes_per_second
        delay = expected_elapsed - (time.monotonic() - self.started_at)
        if delay > 0:
            time.sleep(delay)


def load_config(project):
    path = project / "config" / "dataset.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    sources = {source["name"]: source for source in config["sources"]}
    if set(sources) != {"reviews", "products"}:
        raise ValueError("Dataset config must contain reviews and products")
    return config, sources


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json_atomic(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def response_total_bytes(response, offset):
    content_range = response.headers.get("Content-Range")
    if content_range and "/" in content_range:
        return int(content_range.rsplit("/", 1)[1])
    content_length = response.headers.get("Content-Length")
    return offset + int(content_length) if content_length else None


def load_state(source_dir, source):
    state_path = source_dir / "_state.json"
    if not state_path.exists():
        unexpected = list(source_dir.glob("part-*.jsonl.gz"))
        if unexpected:
            raise RuntimeError(
                f"{source['name']}: parts exist without a checkpoint: {source_dir}"
            )
        return {
            "source_name": source["name"],
            "source_url": source["url"],
            "next_byte_offset": 0,
            "line_count": 0,
            "parts": [],
            "completed": False,
        }

    state = json.loads(state_path.read_text(encoding="utf-8"))
    if state["source_url"] != source["url"]:
        raise RuntimeError(f"{source['name']}: source URL changed since checkpoint")
    for part in state["parts"]:
        part_path = source_dir / part["file_name"]
        if not part_path.is_file() or part_path.stat().st_size != part["compressed_bytes"]:
            raise RuntimeError(f"{source['name']}: checkpoint part is missing: {part_path}")
    return state


def finalize_part(source_dir, temporary_path, part_number, state, part_lines, part_bytes):
    final_path = source_dir / f"part-{part_number:05d}.jsonl.gz"
    os.replace(temporary_path, final_path)
    part = {
        "file_name": final_path.name,
        "line_count": part_lines,
        "uncompressed_bytes": part_bytes,
        "compressed_bytes": final_path.stat().st_size,
        "sha256": sha256_file(final_path),
    }
    state["parts"].append(part)
    write_json_atomic(source_dir / "_state.json", state)
    print(
        f"{state['source_name']}: part {part_number:05d}; "
        f"{state['line_count']:,} lines; "
        f"{state['next_byte_offset'] / (1024 ** 3):.2f} GiB source",
        flush=True,
    )


def download_source(source, output, chunk_bytes, max_bytes_per_second=0):
    source_dir = output / source["name"]
    source_dir.mkdir(parents=True, exist_ok=True)
    state = load_state(source_dir, source)
    if state["completed"]:
        print(f"{source['name']}: already complete", flush=True)
        return state

    offset = state["next_byte_offset"]
    headers = {"User-Agent": USER_AGENT}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = Request(source["url"], headers=headers)

    print(f"{source['name']}: downloading from byte {offset:,}", flush=True)
    limiter = BandwidthLimiter(max_bytes_per_second)
    with urlopen(request, timeout=120) as response:
        status = getattr(response, "status", response.getcode())
        if offset and status != 206:
            raise RuntimeError(
                f"{source['name']}: server did not honor resume range; status={status}"
            )
        total_bytes = response_total_bytes(response, offset)
        part_number = len(state["parts"])
        temporary_path = source_dir / f"part-{part_number:05d}.jsonl.gz.tmp"
        temporary_path.unlink(missing_ok=True)
        part_lines = 0
        part_bytes = 0
        truncated_at = None
        compressed = gzip.open(temporary_path, "wb", compresslevel=1)
        try:
            for raw_line in response:
                limiter.consume(len(raw_line))
                # A line without a newline is only complete at the verified end
                # of the source; otherwise the stream was cut mid-line, and the
                # next run must resume from the start of this line.
                if not raw_line.endswith(b"\n") and (
                    total_bytes is None
                    or state["next_byte_offset"] + len(raw_line) != total_bytes
                ):
                    truncated_at = state["next_byte_offset"]
                    break
                compressed.write(raw_line)
                line_bytes = len(raw_line)
                part_lines += 1
                part_bytes += line_bytes
                state["line_count"] += 1
                state["next_byte_offset"] += line_bytes

                if part_bytes >= chunk_bytes:
                    compressed.close()
                    finalize_part(
                        source_dir,
                        temporary_path,
                        part_number,
                        state,
                        part_lines,
                        part_bytes,
                    )
                    part_number += 1
                    part_lines = 0
                    part_bytes = 0
                    temporary_path = source_dir / f"part-{part_number:05d}.jsonl.gz.tmp"
                    compressed = gzip.open(temporary_path, "wb", compresslevel=1)
        finally:
            compressed.close()

        if part_lines:
            finalize_part(
                source_dir,
                temporary_path,
                part_number,
                state,
                part_lines,
                part_bytes,
            )
        else:
            temporary_path.unlink(missing_ok=True)

    if truncated_at is not None:
        raise RuntimeError(
            f"{source['name']}: stream ended mid-line at byte {truncated_at}; "
            "rerun to resume from the start of that line"
        )
    if total_bytes is not None and state["next_byte_offset"] != total_bytes:
        raise RuntimeError(
            f"{source['name']}: received {state['next_byte_offset']} of "
            f"{total_bytes} source bytes"
        )

    state["completed"] = True
    state["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    state["source_bytes"] = state["next_byte_offset"]
    state["compressed_bytes"] = sum(
        part["compressed_bytes"] for part in state["parts"]
    )
    write_json_atomic(source_dir / "_state.json", state)
    write_json_atomic(source_dir / "manifest.json", state)
    print(
        f"{source['name']}: COMPLETE; {state['line_count']:,} lines; "
        f"{state['compressed_bytes'] / (1024 ** 3):.2f} GiB compressed",
        flush=True,
    )
    return state


def main():
    args = parse_args()
    if args.chunk_mib <= 0:
        raise ValueError("--chunk-mib must be positive")
    if args.max_mib_per_second < 0:
        raise ValueError("--max-mib-per-second must be non-negative")

    project = Path(__file__).resolve().parent.parent
    output = args.output or project / "data" / "full"
    output.mkdir(parents=True, exist_ok=True)
    config, sources = load_config(project)
    chunk_bytes = args.chunk_mib * 1024 * 1024
    max_bytes_per_second = args.max_mib_per_second * 1024 * 1024

    manifests = {
        name: download_source(
            sources[name], output, chunk_bytes, max_bytes_per_second
        )
        for name in ("reviews", "products")
    }
    full_manifest = {
        "dataset_id": config["dataset_id"],
        "source_reference": config["source_reference"],
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "chunk_uncompressed_mib": args.chunk_mib,
        "sources": manifests,
    }
    write_json_atomic(output / "full_manifest.json", full_manifest)
    (output / "_COMPLETE").write_text(
        config["source_reference"] + "\n",
        encoding="utf-8",
    )
    print(f"FULL DATASET READY: {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
