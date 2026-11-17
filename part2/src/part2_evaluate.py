"""Evaluate independently hand-labeled 50-row CSV."""

import argparse
import csv

p = argparse.ArgumentParser()

p.add_argument(
    "--labeled-csv",
    required=True,
    help="CSV filled by a human with manual_label",
)

a = p.parse_args()

classes = {"World", "Sports", "Business", "Sci/Tech"}
allowed = classes | {"None"}

with open(a.labeled_csv, encoding="utf-8-sig", newline="") as f:
    rows = list(csv.DictReader(f))

if len(rows) != 50:
    raise SystemExit(f"Expected exactly 50 rows, found {len(rows)}")

bad = [
    (i, r.get("manual_label", ""))
    for i, r in enumerate(rows, 1)
    if (r.get("manual_label") or "").strip() not in allowed
]

if bad:
    raise SystemExit(
        f"Invalid manual labels: {bad[:10]}"
    )

# Validate model predictions too.
invalid_predictions = [
    (i, r.get("predicted_class", ""))
    for i, r in enumerate(rows, 1)
    if (r.get("predicted_class") or "").strip() not in classes
]

if invalid_predictions:
    raise SystemExit(
        f"Invalid predictions: {invalid_predictions[:10]}"
    )

# Exclude messages manually labeled None.
usable = [
    r for r in rows
    if r["manual_label"].strip() != "None"
]

none_count = len(rows) - len(usable)

correct = sum(
    r["manual_label"].strip() == r["predicted_class"].strip()
    for r in usable
)

accuracy = correct / len(usable) if usable else None

print(f"Total hand-labeled: {len(rows)}")
print(f"None of these: {none_count}")
print(f"Four-class eligible: {len(usable)}")
print(f"Correct within four classes: {correct}")

if accuracy is not None:
    print(f"Hand-check accuracy: {accuracy:.2%}")
else:
    print("Hand-check accuracy: N/A")

print("\nMisclassified messages:")

for i, r in enumerate(usable, 1):
    human = r["manual_label"].strip()
    predicted = r["predicted_class"].strip()

    if human != predicted:
        print(
            f"human={human} predicted={predicted} "
            f":: {r['text'][:180]}"
        )

print("\nMessages labeled None:")

for r in rows:
    if r["manual_label"].strip() == "None":
        print(
            f"predicted={r['predicted_class']} "
            f":: {r['text'][:180]}"
        )
