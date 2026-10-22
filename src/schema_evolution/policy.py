import json
from pathlib import Path

from schema_evolution.classifier import classify_changes


ACCEPT = "ACCEPT"
REVIEW = "REVIEW"
BLOCK = "BLOCK"

# Thứ tự tăng dần theo mức nghiêm trọng của hành động.
ACTION_ORDER = [ACCEPT, REVIEW, BLOCK]

DEFAULT_POLICY_PATH = Path(__file__).with_name("policy.json")


def load_policy(path=DEFAULT_POLICY_PATH):
    document = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return document["severity_actions"]


def decide(changes, severity_actions=None, dependencies=None):
    # Trả về hành động tệ nhất trong các thay đổi, kèm hành động từng thay đổi.
    if severity_actions is None:
        severity_actions = load_policy()

    decided = []

    for change in classify_changes(changes, dependencies):
        # Severity chưa có trong policy thì chặn, không cho qua.
        action = severity_actions.get(change["severity"], BLOCK)
        decided.append({**change, "action": action})

    overall = max(
        (change["action"] for change in decided),
        key=ACTION_ORDER.index,
        default=ACCEPT,
    )
    return {"action": overall, "changes": decided}
