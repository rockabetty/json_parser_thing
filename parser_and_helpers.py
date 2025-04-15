import json
from typing import Generator, Any, Union, List, Dict
from pathlib import Path
from contextlib import contextmanager
import logging
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def raise_if_not_list(source):
    if not isinstance(source, list):
        raise ValueError("Top-level JSON needs to be an array.")


@contextmanager
def json_processing_context(label="JSON Processing"):
    logger.info(f">>>>>> START: {label}")
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        logger.info(f">>>>>> END: {label} complete in {end - start:.4f} seconds")


def json_event_stream(datastring: str) -> Generator[Any, None, None]:
    try:
        yield from yolo_json_event_stream(datastring)
    except UnicodeDecodeError:
        logger.error("File contained bad encoding.")
    except json.JSONDecodeError:
        logger.error("Invalid JSON.")
    except FileNotFoundError:
        logger.error(f"File not found at path: {datastring}.")
    except KeyboardInterrupt:
        logger.warning(f"User cancelled while processing JSON.")
    except ValueError as ve:
        logger.error(f"Value Error: {ve}")

    finally:
        logger.info("Done processing data.")


def yolo_json_event_stream(datastring: str) -> Generator[Any, None, None]:
    """
    Input: Either a filepath with valid JSON array, or said JSON array
    Output: A generator of json 'events' or a field of nightmares in the
    event of bad data.
    """

    if not isinstance(datastring, str):
        raise ValueError("Please provide a string containing a filepath or valid JSON.")

    trimmed = datastring.strip()
    loaded_list = []

    if trimmed.startswith("["):
        loaded_list = json.loads(trimmed)
    elif trimmed.startswith("{"):
        raise ValueError("Top level JSON needs to be an array.")
    else:
        data_path = Path(trimmed)
        loaded_list = json.loads(data_path.read_text())

    raise_if_not_list(loaded_list)
    if len(loaded_list) == 0:
        logger.warning("No expressions found in list.")

    for item in loaded_list:
        yield item


def source_too_large_msg(threshold):
    return (
        f"Your data is larger than the threshold ({threshold} mb)."
        " Your options are to use json_event_stream() instead,"
        " raise threshold_mb, or set process_ovesized to True."
        " Using json_event_stream() is likely the best option."
    )


def load_json_string(
    datastring: str = "",
    process_oversized: bool = False,
    threshold_mb: int = 100,
):
    """
    Loads a JSON string into memory.
    Output: Processed JSON array.
    Args:
        datastring (str): Must contain valid JSON array at top level.

        process_oversized (bool): If true, ignores threshold_mb, loading anyway.

        threshold_mb (int): max megabytes before warning about size if
        'disregard_data_size' is false.'
    """
    if not isinstance(datastring, str):
        raise ValueError("Please provide a string to load_json_string.")

    if process_oversized:
        return list(json_event_stream(datastring))

    json_bytes = datastring.encode("utf-8")
    size_mb = len(json_bytes) / (1024 * 1024)

    if size_mb > threshold_mb:
        warning_msg = source_too_large_msg(threshold_mb)
        logger.warning(warning_msg)
        raise ValueError(warning_msg)

    return list(json_event_stream(datastring))


def load_json_file(
    datastring: str = "",
    process_oversized: bool = False,
    threshold_mb: int = 100,
):
    """
    Loads JSON from a file into memory.
    Output: Processed JSON array.
    Args:
        datastring (str): Expects file to contain valid JSON array.

        process_oversized (bool): If true, ignore threshold_mb, loading anyway.

        threshold_mb: Max megabytes before warning about size. Will not process
        JSON if the file size exceeds threshold_mb & process_oversized = False.
    """
    trimmed = datastring.strip()
    if process_oversized:
        return list(json_event_stream(trimmed))

    if os.path.isfile(trimmed):
        file_size_mb = os.path.getsize(trimmed) / (1024 * 1024)
        if file_size_mb > threshold_mb:
            warning_msg = source_too_large_msg(threshold_mb)
            logger.warning(warning_msg)
            raise ValueError(warning_msg)
        return list(json_event_stream(trimmed))
    else:
        raise FileNotFoundError(f"{datastring} does not lead to a file.")


def load_json_data(
    datastring: str = "",
    process_oversized: bool = False,
    threshold_mb: int = 100,
):
    """
    Loads JSON into memory. Determines whether to call load_json_file() or
    load_json_string(), passing arguments along to either one accordingly.
    Args:
        filepath (str): Expects file to contain valid JSON array.

        process_oversized (bool): If true, ignore threshold_mb, loading anyway.

        threshold_mb: Max megabytes before warning about size. Will not process
        JSON if the file size exceeds threshold_mb & process_oversized = False.
    """

    if process_oversized:
        return list(json_event_stream(datastring))

    trimmed = datastring.strip()
    # Right now, load_json_string will reject things that start with "{"
    # but maybe future behavior accepts an object or an array.
    if trimmed.startswith("[") or trimmed.startswith("{"):
        return load_json_string(datastring, process_oversized, threshold_mb)
    return load_json_file(datastring, process_oversized, threshold_mb)


def extract_events(
    data_source: Union[List[Dict[str, Any]], Dict[str, Any]], parent_key=""
) -> List[Dict[str, Any]]:
    """
    Extracts events from a JSON object which may be nested in case you give me
    something insane
    Args:
        data_source: A JSON object or array of objects.
        parent_key: Optional parent context to track hierarchy.
    Returns:
        A list of event dictionaries.
    """
    events = []

    if isinstance(data_source, list):
        return data_source  # because it's already flat.

    elif isinstance(data_source, dict):
        # this could just be an event itself and events probably will be timestamped?
        if "timestamp" in data_source:
            return [data_source]

        # Otherwise it's a container of events, uh, hopefully.
        for key, value in data_source.items():
            nested_events = extract_events(value, key)

            if parent_key and nested_events:
                for event in nested_events:
                    if isinstance(event, dict) and "parent_key" not in event:
                        event["parent_key"] = parent_key

            events.extend(nested_events)

    return events


def sort_by_timestamp(
    events: list, timestamp_key: str = "timestamp", ascending: bool = True
):
    if not isinstance(events, list):
        raise ValueError("sort_by_timestamp requires a list.")

    sortable_events = [event for event in events if event.get(timestamp_key)]

    if len(sortable_events) == 0:
        logger.warning("List did not contain any timestamped events.")
        return None

    return sorted(
        sortable_events,
        # could probably make this more robust IRL but I don't think a tech screen
        # is going to need something much more than this
        key=lambda x: datetime.strptime(x.get(timestamp_key), "%Y-%m-%dT%H:%M:%S"),
        reverse=not ascending,
    )


def filter_events_by_date_range(
    events: list, start_date: str, end_date: str, timestamp_key: str = "timestamp"
):
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date + "T23:59:59")

    def is_in_range(event):
        ts = event.get(timestamp_key)
        if not ts:
            return False

        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        return start <= ts <= end

    return [event for event in events if is_in_range(event)]


def convert_events_to_dataframe(
    data_source: Union[str, List[Dict[str, Any]], Dict[str, Any]],
    process_oversized: bool = True,
    threshold_mb: int = 100,
):
    """
    Convert JSON events data to a pandas DataFrame.
    Works with file path, JSON string, or parsed data.
    The import is local because I'm not really certain we'd even need it
    and it's a big import so I just kind of have this little guy chillin'.
    """
    import pandas as pd

    if isinstance(data_source, str):
        events = load_json_data(data_source, process_oversized, threshold_mb)
    else:
        events = extract_events(data_source)

    df = pd.DataFrame(events)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df
