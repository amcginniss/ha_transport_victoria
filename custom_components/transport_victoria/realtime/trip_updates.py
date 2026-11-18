"""Parse GTFS Realtime trip-update feeds into per-stop delay maps.

The trip-updates feed carries delay and cancellation information keyed by
trip_id + stop_id. This module converts raw FeedMessage objects into a simple
dict so the coordinator can merge realtime data into scheduled departures
without touching protobuf objects outside this module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from homeassistant.util import dt as dt_util

_LOGGER = logging.getLogger(__name__)

# ScheduleRelationship enum values from the GTFS Realtime spec
_SR_CANCELED = 3


@dataclass(slots=True, frozen=True)
class StopDelay:
    """Realtime delay information for one trip at one stop."""

    delay_seconds: int
    is_cancelled: bool
    # Absolute predicted POSIX time. Metro Trains' feed sends only this, with
    # no ``delay`` field, so the delay must be derived from the schedule.
    event_time: int | None = None
    has_delay: bool = False


def lookup_stop_delay(
    trip_delays: dict[str, StopDelay],
    trip_id: str,
    stop_id: str,
    stop_sequence: int | None,
) -> StopDelay | None:
    """Find the realtime entry for a trip at a stop, by stop_id then stop_sequence."""
    found = trip_delays.get(f"{trip_id}:{stop_id}")
    if found is None and stop_sequence is not None:
        found = trip_delays.get(f"{trip_id}#{stop_sequence}")
    return found


def effective_delay(stop_delay: StopDelay | None, scheduled: datetime) -> int:
    """Return the delay in seconds, deriving it from ``event_time`` if needed."""
    if stop_delay is None:
        return 0
    if stop_delay.has_delay or not stop_delay.event_time:
        return stop_delay.delay_seconds
    if scheduled.tzinfo is None:
        scheduled = scheduled.replace(tzinfo=dt_util.get_default_time_zone())
    return int(stop_delay.event_time - scheduled.timestamp())


def parse_trip_updates(feed: object) -> dict[str, StopDelay]:
    """Parse a TripUpdates FeedMessage into a delay lookup dict.

    The returned dict is keyed by ``"<trip_id>:<stop_id>"`` so the coordinator
    can do an O(1) lookup for each scheduled departure.

    When a stop_time_update carries both arrival and departure delays, the
    departure delay is preferred (it's what passengers care about). If only
    arrival is present, that value is used instead.

    Args:
        feed: A decoded ``gtfs_realtime_pb2.FeedMessage`` instance.

    Returns:
        Dict mapping "trip_id:stop_id" → StopDelay.
    """
    result: dict[str, StopDelay] = {}

    try:
        entities = feed.entity  # type: ignore[attr-defined]
    except AttributeError:
        _LOGGER.warning("parse_trip_updates received unexpected feed type: %s", type(feed))
        return result

    for entity in entities:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip_id: str = trip_update.trip.trip_id

        if not trip_id:
            continue

        is_cancelled = (
            trip_update.trip.schedule_relationship == _SR_CANCELED
        )

        for stu in trip_update.stop_time_update:
            stop_id: str = stu.stop_id
            if not stop_id:
                continue

            delay = 0
            has_delay = False
            event_time: int | None = None
            if stu.HasField("departure") and stu.departure.HasField("delay"):
                delay = stu.departure.delay
                has_delay = True
            elif stu.HasField("arrival") and stu.arrival.HasField("delay"):
                delay = stu.arrival.delay
                has_delay = True
            if stu.HasField("departure") and stu.departure.time:
                event_time = stu.departure.time
            elif stu.HasField("arrival") and stu.arrival.time:
                event_time = stu.arrival.time

            key = f"{trip_id}:{stop_id}"
            stop_delay = StopDelay(
                delay_seconds=delay,
                is_cancelled=is_cancelled,
                event_time=event_time,
                has_delay=has_delay,
            )
            result[key] = stop_delay
            # Secondary key so a platform stop_id mismatch can still match
            # on the trip's stop sequence.
            if stu.stop_sequence:
                result.setdefault(f"{trip_id}#{stu.stop_sequence}", stop_delay)

    _LOGGER.debug("Parsed %d stop-level trip updates", len(result))
    return result
