"""Generate a CSV of call and message history from a Google Voice Takeout."""
import argparse
import csv
import functools
import hashlib
import operator
import os
import re
import sys
import zipfile
from contextlib import contextmanager
from datetime import datetime
from typing import IO, Any, Dict, Iterable, Iterator, List, Optional
from xml.etree import ElementTree as ET

CALL_PATTERN = (
    r"Takeout/Voice/(?P<directory>Calls|Spam)/"
    r"(?P<contact>.*?) - (?P<type>.+?) - (?P<timestamp>.+?)\.html"
)

CALL_FIELDS = [
    "timestamp",
    "date",
    "time",
    "type",
    "contact_id",
    "contact_name",
    "call_duration",
    "message_days",
    "message_count",
]


# HACK: This is actually `Dict[str, Optional[Union[str, int]]]`
# But that would require lots of things like `cast(str, ...)` to pass mypy
# Might be better to use a dataclass
CallDict = Dict[str, Any]


@contextmanager
def pipeable() -> Iterator[None]:
    """
    Silence noisy errors from `python my_script.py | head`.

    https://docs.python.org/3/library/signal.html#note-on-sigpipe
    """
    try:
        yield
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)


@pipeable()
def main() -> None:
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


def parse_takeout(path: str) -> List[CallDict]:
    with zipfile.ZipFile(path) as takeout:
        calls = match_calls(takeout.namelist())
        calls = parse_calls(calls, takeout)
        calls = sorted(calls, key=operator.itemgetter("timestamp"))

    contact_total = sum(CONTACT_STATS[k] for k in ["missing", "numbers", "names"])
    if contact_total != CONTACT_STATS["total"]:
        raise ValueError(
            f"Total contacts ({CONTACT_STATS['total']}) != "
            f"sum of each type ({contact_total})"
        )

    return calls


def match_calls(filenames: List[str]) -> Iterable[CallDict]:
    for filename in filenames:
        match = re.match(CALL_PATTERN, filename)
        if not match:
            continue

        yield {
            "filename": filename,
            **match.groupdict(),
        }


def parse_calls(
    calls: Iterable[CallDict],
    takeout: zipfile.ZipFile,
) -> Iterable[CallDict]:
    for call in calls:
        yield {
            **call,
            "timestamp": format_timestamp(call["timestamp"]),
            **parse_contact(call["contact"]),
            **parse_file(call["filename"], takeout),
        }


def parse_contact(contact: str) -> CallDict:
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


ANONYMIZED_VALUES: Dict[str, str] = {}


@functools.lru_cache()
def anonymize(value: str) -> str:
    # Using a small digest_size for readability
    # In initial testing, didn't get collisions until digest_size == 2
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=5).hexdigest()

    current_value = ANONYMIZED_VALUES.setdefault(digest, value)
    if current_value != value:
        raise ValueError(f"Duplicate anonymization for {current_value} and {value}")

    return digest


def format_timestamp(timestamp: str) -> str:
    # 2020-08-21T18_57_10Z => 2020-08-21T18:57:10-04:00
    # Matching datetime.isoformat(). For details, see:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.isoformat
    return timestamp.replace("_", ":").replace("Z", "+00:00")


def parse_file(filename: str, takeout: zipfile.ZipFile) -> CallDict:
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
        "call_duration": get_duration(xml),
        **format_datetime(get_published(xml)),
        **parse_messages(xml),
    }


def get_duration(xml: ET.Element) -> Optional[str]:
    # <abbr class="duration" title="PT2M23S">(00:02:23)</abbr>
    element = xml.find(".//*[@class='duration']")
    if element is None or element.text is None:
        return None

    return element.text.strip("()")


def get_published(xml: ET.Element) -> Optional[datetime]:
    # <abbr class="published" title="2020-06-14T12:40:38.000-04:00">
    #     Jun 14, 2020, 12:40:38 PM Eastern Time
    # </abbr>
    element = xml.find(".//*[@class='published']")
    if element is None:
        return None

    return datetime.fromisoformat(element.get("title", ""))


def format_datetime(dt: Optional[datetime]) -> CallDict:
    if dt is None:
        return {}

    return {
        "date": dt.strftime("%Y-%m-%d"),
        "time": dt.strftime("%I:%M %p"),
    }


def parse_messages(xml: ET.Element) -> CallDict:
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

    first_dt = get_dt(messages[0])
    last_dt = get_dt(messages[-1])

    if not (first_dt and last_dt):
        return {}

    return {
        **format_datetime(first_dt),
        "message_days": (last_dt - first_dt).days,
        "message_count": len(messages),
    }


def get_dt(xml: ET.Element) -> Optional[datetime]:
    # <abbr class="dt" title="2020-06-23T21:10:00.971-04:00">
    element = xml.find(".//*[@class='dt']")
    if element is None:
        return None

    return datetime.fromisoformat(element.get("title", ""))


def write_csv(
    calls: List[CallDict],
    fieldnames: List[str],
    csvfile: IO[str],
) -> None:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for call in calls:
        writer.writerow(call)


if __name__ == "__main__":
    main()
