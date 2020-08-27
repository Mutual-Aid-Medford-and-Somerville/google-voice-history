"""Generate a CSV of logs from a Google Voice Takeout."""
import argparse
import csv
import functools
import hashlib
import operator
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta

CALL_PATTERN = (
    r"Takeout/Voice/(?P<directory>Calls|Spam)/"
    r"(?P<contact>.*?) - (?P<type>.+?) - (?P<timestamp>.+?)\.html"
)

CALL_FIELDS = [
    "timestamp",
    "type",
    "contact_id",
    "contact_name",
    "duration",
    "messages",
]


def main():
    # TODO: Add option for writing to a file?
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "takeout_path", help="File path of Google Voice Takeout", metavar="PATH"
    )
    args = parser.parse_args()

    calls = parse_takeout(args.takeout_path)
    write_csv(calls, CALL_FIELDS, sys.stdout)


CONTACT_STATS = {
    "total": 0,
    "missing": 0,
    "numbers": 0,
    "names": 0,
}


def parse_takeout(path):
    with zipfile.ZipFile(path) as takeout:
        calls = match_calls(takeout.namelist())
        calls = (
            {
                **call,
                "timestamp": parse_timestamp(call["timestamp"]),
                **parse_contact(call["contact"]),
                **parse_file(call["filename"], takeout),
            }
            for call in calls
        )
        calls = sorted(calls, key=operator.itemgetter("timestamp"))

    contact_total = sum(CONTACT_STATS[k] for k in ["missing", "numbers", "names"])
    if contact_total != CONTACT_STATS["total"]:
        raise ValueError(
            f"Total contacts ({CONTACT_STATS['total']}) != "
            f"sum of each type ({contact_total})"
        )

    return calls


def match_calls(filenames):
    for filename in filenames:
        match = re.match(CALL_PATTERN, filename)
        if not match:
            continue

        yield {
            "filename": filename,
            **match.groupdict(),
        }


def parse_contact(contact):
    CONTACT_STATS["total"] += 1

    is_number = re.search(r"\d{10}", contact) is not None

    if is_number:
        CONTACT_STATS["numbers"] += 1
    elif contact:
        CONTACT_STATS["names"] += 1
    else:
        CONTACT_STATS["missing"] += 1

    return {
        "contact_id": anonymize(contact) if contact else None,
        "contact_name": contact if not is_number else None,
    }


ANONYMIZED_VALUES = {}


@functools.lru_cache()
def anonymize(value):
    # Using a small digest_size for readability
    # In initial testing, didn't get collisions until digest_size == 2
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=5).hexdigest()

    current_value = ANONYMIZED_VALUES.setdefault(digest, value)
    if current_value != value:
        raise ValueError(f"Duplicate anonymization for {current_value} and {value}")

    return digest


def parse_timestamp(timestamp):
    # 2020-08-21T18_57_10Z => 2020-08-21T18:57:10-04:00
    return (
        datetime.strptime(timestamp.replace("Z", "UTC"), "%Y-%m-%dT%H_%M_%S%Z")
        .astimezone()
        .isoformat()
    )


def parse_file(filename, takeout):
    content = takeout.read(filename).decode("utf-8")

    try:
        xml = ET.fromstring(
            # HACK: Remove HTML tags and entities that cause XML parse errors
            # This is fine because we don't care about the content
            # Alternatively, use a proper HTML parser like lxml.html or pyquery
            content.replace("<br>", "").replace("&", ""),
        )
    except ET.ParseError as exc:
        raise ValueError(f"Error parsing {filename}") from exc

    return {
        "duration": parse_duration(xml),
        **parse_messages(xml),
    }


def parse_duration(xml):
    # <abbr class="duration" title="PT2M23S">(00:02:23)</abbr>
    element = xml.find(".//*[@class='duration']")
    if element is None:
        return None

    hours, minutes, seconds = (int(x) for x in element.text.strip("()").split(":"))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def parse_messages(xml):
    # <div class="hChatLog hfeed">
    #     <div class="message">
    #         <abbr class="dt" title="2020-06-23T21:10:00.971-04:00">
    #             Jun 23, 2020, 9:10:00 PM Eastern Time
    #         </abbr>
    #         <!-- ... -->
    #     </div>
    #     <!-- ... -->
    # </div>
    messages = xml.findall(".//*[@class='message']")
    if not messages:
        return {}

    timestamps = [parse_dt(element) for element in messages]

    return {
        "messages": len(messages),
        "duration": timestamps[-1] - timestamps[0],
    }


def parse_dt(xml):
    # <abbr class="dt" title="2020-06-23T21:10:00.971-04:00">
    element = xml.find(".//*[@class='dt']")
    if element is None:
        return None

    return datetime.fromisoformat(element.get("title")).replace(microsecond=0)


def write_csv(calls, fieldnames, csvfile):
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for call in calls:
        writer.writerow(call)


if __name__ == "__main__":
    main()
