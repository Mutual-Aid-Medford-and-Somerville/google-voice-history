"""Generate a CSV of logs from a Google Voice Takeout."""
import argparse
import csv
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

CALL_FIELDS = ["timestamp", "type", "contact"]


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


def match_calls(filenames):
    for filename in filenames:
        match = re.match(CALL_PATTERN, filename)
        if not match:
            continue

        yield match.groupdict()


def process_call(call):
    return {
        **call,
        "timestamp": normalize_timestamp(call["timestamp"]),
    }
    # TODO: Anonymize `contact`?
    # TODO: Get duration from contents of `filename`


def normalize_timestamp(timestamp):
    # 2020-08-21T18_57_10Z => 2020-08-21T18:57:10-04:00
    return (
        datetime.strptime(timestamp.replace("Z", "UTC"), "%Y-%m-%dT%H_%M_%S%Z")
        .astimezone()
        .isoformat()
    )


def write_csv(calls, csvfile):
    writer = csv.DictWriter(csvfile, fieldnames=CALL_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for call in calls:
        writer.writerow(call)


if __name__ == "__main__":
    main()
