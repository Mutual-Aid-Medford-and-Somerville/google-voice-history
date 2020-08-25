"""Generate a CSV of logs from a Google Voice Takeout."""
import argparse
import csv
import functools
import hashlib
import operator
import re
import sys
import zipfile
from datetime import datetime

CALL_PATTERN = (
    r"(?P<filename>Takeout/Voice/Calls/"
    r"(?P<contact>.*?) - (?P<type>.+?) - (?P<timestamp>.+?)"
    r"\.html)"
)

CALL_FIELDS = ["timestamp", "type", "contact_id", "contact_name"]

CONTACT_STATS = {
    "total": 0,
    "missing": 0,
    "numbers": 0,
    "names": 0,
}

CALL_NUMBERS = set()
CALL_NAMES = set()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "takeout_path", help="File path of Google Voice Takeout", metavar="PATH"
    )
    args = parser.parse_args()

    takeout = zipfile.ZipFile(args.takeout_path)

    calls = match_calls(takeout.namelist())
    calls = (process_call(call) for call in calls)
    calls = sorted(calls, key=operator.itemgetter("timestamp"))

    # TODO: Add option for writing to a file?
    write_csv(calls, sys.stdout)

    assert CONTACT_STATS["total"] == sum(
        CONTACT_STATS[k] for k in ["missing", "numbers", "names"]
    )
    print(f"calls: {CONTACT_STATS['total']}", file=sys.stderr)
    print(f"numbers: {len(CALL_NUMBERS)}", file=sys.stderr)
    print(f"names: {len(CALL_NAMES)}", file=sys.stderr)


def match_calls(filenames):
    for filename in filenames:
        match = re.match(CALL_PATTERN, filename)
        if not match:
            continue

        yield match.groupdict()


def process_call(call):
    CONTACT_STATS["total"] += 1

    contact = call["contact"]
    is_number = re.search(r"\d{10}", contact) is not None

    if is_number:
        CONTACT_STATS["numbers"] += 1
        CALL_NUMBERS.add(contact)
    elif contact:
        CONTACT_STATS["names"] += 1
        CALL_NAMES.add(contact)
    else:
        CONTACT_STATS["missing"] += 1

    # TODO: Get duration from contents of `call["filename"]`
    return {
        "timestamp": normalize_timestamp(call["timestamp"]),
        "type": call["type"],
        "contact_id": anonymize(contact),
        "contact_name": contact if not is_number else None,
    }


@functools.lru_cache()
def anonymize(value):
    # Using a small digest_size for readability
    # In initial testing, didn't get collisions until digest_size == 2
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=5).hexdigest()

    current_value = ANONYMIZED_VALUES.setdefault(digest, value)
    assert current_value == value, f"Digest {digest} already used"

    return digest


ANONYMIZED_VALUES = {}


def normalize_timestamp(timestamp):
    # 2020-08-21T18_57_10Z => 2020-08-21T18:57:10-04:00
    return (
        datetime.strptime(timestamp.replace("Z", "UTC"), "%Y-%m-%dT%H_%M_%S%Z")
        .astimezone()
        .isoformat()
    )


def write_csv(calls, csvfile):
    writer = csv.DictWriter(csvfile, fieldnames=CALL_FIELDS)
    writer.writeheader()
    for call in calls:
        writer.writerow(call)


if __name__ == "__main__":
    main()
