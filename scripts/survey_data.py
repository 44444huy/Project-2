import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_LIMIT = 5000


def positive_int(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("limit must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("limit must be a positive integer")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download and profile the first N Electronics rows per source."
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        default=positive_int(os.environ.get("SAMPLE_SIZE", DEFAULT_LIMIT)),
        help="rows per source (default: SAMPLE_SIZE or 5000)",
    )
    return parser.parse_args()


def load_dataset_config(project):
    config_path = project / "config" / "dataset.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    sources = {source["name"]: source for source in config["sources"]}
    if set(sources) != {"reviews", "products"}:
        raise ValueError("Dataset config must contain reviews and products")
    return config, sources


def survey(source, output, limit):
    name = source["name"]
    url = source["url"]
    present, nulls, empty = Counter(), Counter(), Counter()
    types = defaultdict(Counter)
    scanned = valid = invalid = 0
    errors = []
    sample_path = output / f"{name}_sample.jsonl"
    progress_interval = max(1000, min(10000, limit // 10))

    print(f"\nReading {name}: first {limit:,} lines...", flush=True)
    request = Request(url, headers={"User-Agent": "Project2-data-survey/1.0"})

    with urlopen(request, timeout=90) as response:
        with sample_path.open("wb") as sample:
            for raw_line in response:
                scanned += 1
                sample.write(raw_line)

                try:
                    row = json.loads(raw_line)
                    if not isinstance(row, dict):
                        raise ValueError("Expected a JSON object")
                except (ValueError, UnicodeDecodeError) as error:
                    invalid += 1
                    if len(errors) < 5:
                        errors.append({"line": scanned, "error": str(error)})
                else:
                    valid += 1
                    for field, value in row.items():
                        present[field] += 1
                        types[field][type(value).__name__] += 1
                        if value is None:
                            nulls[field] += 1
                        elif value == "" or value == [] or value == {}:
                            empty[field] += 1

                if scanned % progress_interval == 0:
                    print(f"  {name}: {scanned:,} lines", flush=True)
                if scanned >= limit:
                    break

    if scanned != limit:
        raise ValueError(f"{name}: expected {limit:,} lines, received {scanned:,}")

    profile = {
        "source_url": url,
        "sampling_method": "first N lines; not a representative sample",
        "profiling_scope": "top-level fields",
        "sample_limit": limit,
        "scanned_lines": scanned,
        "valid_objects": valid,
        "invalid_lines": invalid,
        "sample_bytes": sample_path.stat().st_size,
        "fields": {
            field: {
                "types": dict(types[field]),
                "missing": valid - present[field],
                "null": nulls[field],
                "empty": empty[field],
                "missing_or_null_pct": round(
                    100 * (valid - present[field] + nulls[field]) / valid, 2
                ) if valid else None,
            }
            for field in sorted(present)
        },
        "parse_error_examples": errors,
    }

    (output / f"{name}_profile.json").write_text(
        json.dumps(profile, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"{name}: {valid:,} valid; {invalid:,} invalid", flush=True)
    return profile


def build_report(profiles, limit):
    report = [
        f"# Amazon Electronics: first {limit:,} rows profiling",
        "",
        "Samples contain the first N lines, not a random sample.",
        "Missing/null percentages use valid JSON objects as denominator.",
        "Independent samples cannot measure full-dataset join coverage.",
        "",
    ]
    for name, profile in profiles.items():
        report.extend([
            f"## {name}",
            "",
            f"Scanned: {profile['scanned_lines']}; "
            f"valid: {profile['valid_objects']}; "
            f"invalid: {profile['invalid_lines']}.",
            "",
            "| Field | Types | Missing | Null | Empty | Missing/null % |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ])
        for field, info in profile["fields"].items():
            label = field.replace("|", r"\|").replace("\n", " ")
            observed = ", ".join(
                f"{kind}: {count}" for kind, count in info["types"].items()
            )
            report.append(
                f"| {label} | {observed} | {info['missing']} | "
                f"{info['null']} | {info['empty']} | "
                f"{info['missing_or_null_pct']} |"
            )
        report.append("")
    return "\n".join(report)


def main():
    args = parse_args()
    project = Path(__file__).resolve().parent.parent
    output = project / "data_survey" / f"first_{args.limit}"
    if output.exists():
        print(f"FAILED: output already exists: {output}", flush=True)
        return 2

    output.mkdir(parents=True)
    try:
        _, sources = load_dataset_config(project)
        profiles = {
            name: survey(sources[name], output, args.limit)
            for name in ("reviews", "products")
        }
        (output / "summary.md").write_text(
            build_report(profiles, args.limit), encoding="utf-8"
        )
    except Exception as error:
        print(f"\nFAILED: {error}\nPartial files: {output}", flush=True)
        return 1

    print(f"\nDONE. Results: {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
