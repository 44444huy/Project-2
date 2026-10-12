import json
from collections import Counter, defaultdict
from pathlib import Path
from urllib.request import Request, urlopen

LIMIT = 5000
BASE = (
    "https://huggingface.co/datasets/"
    "McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
)
SOURCES = {
    "reviews": "raw/review_categories/Electronics.jsonl",
    "products": "raw/meta_categories/meta_Electronics.jsonl",
}


def survey(name, source_path, output):
    url = BASE + source_path
    present, nulls, empty = Counter(), Counter(), Counter()
    types = defaultdict(Counter)
    scanned = valid = invalid = 0
    errors = []

    print(f"\nReading {name}: first {LIMIT:,} lines...", flush=True)
    request = Request(url, headers={"User-Agent": "Project2-data-survey/1.0"})

    with urlopen(request, timeout=90) as response:
        with (output / f"{name}_sample.jsonl").open("wb") as sample:
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

                if scanned % 1000 == 0:
                    print(f"  {name}: {scanned:,} lines", flush=True)
                if scanned >= LIMIT:
                    break

    if scanned == 0:
        raise ValueError(f"{name}: source returned no data")

    profile = {
        "source_url": url,
        "sampling_method": "first N lines; not a representative sample",
        "profiling_scope": "top-level fields",
        "scanned_lines": scanned,
        "valid_objects": valid,
        "invalid_lines": invalid,
        "sample_bytes": (output / f"{name}_sample.jsonl").stat().st_size,
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


def main():
    project = Path(__file__).resolve().parent.parent
    output = project / "data_survey" / "first_5000"
    output.mkdir(parents=True, exist_ok=False)
    try:
        profiles = {
            name: survey(name, path, output)
            for name, path in SOURCES.items()
        }

        report = [
            "# Amazon Electronics: initial data profiling",
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

        (output / "summary.md").write_text(
            "\n".join(report), encoding="utf-8"
        )
    except Exception as error:
        print(f"\nFAILED: {error}\nPartial files: {output}", flush=True)
        return 1
    print(f"\nDONE. Results: {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
