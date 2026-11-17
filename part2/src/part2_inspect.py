import argparse
import csv
from collections import Counter
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="outputs/part2/all_predictions"
    )
    args = parser.parse_args()

    files = sorted(Path(args.input).glob("part-*.csv"))

    if not files:
        raise SystemExit("No prediction CSV files found.")

    counts = Counter()
    examples = []

    for path in files:
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                counts[row["predicted_class"]] += 1

                if len(examples) < 3:
                    examples.append(row)

    print("===== Classification Summary =====")

    for name in ("World", "Sports", "Business", "Sci/Tech"):
        print(f"{name:10}: {counts[name]}")

    print(f"{'Total':10}: {sum(counts.values())}")

    print("\n===== Sample Predictions =====")

    for i, row in enumerate(examples, 1):
        print(f"\nExample {i}")
        print(f"Text: {row['text'][:120]}")
        print(f"Category: {row['predicted_class']}")
        print(f"Probability: {row['predicted_probability']}")


if __name__ == "__main__":
    main()
