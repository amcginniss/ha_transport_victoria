"""Actions (services) for Transport Victoria."""

from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from pathlib import Path

import voluptuous as vol

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.util import dt as dt_util

from .const import (
    CONF_ENTRY_TYPE,
    CONF_MODE,
    CONF_STOP_ID,
    DOMAIN,
    ENTRY_TYPE_ROUTE_TRACKER,
    ENTRY_TYPE_VEHICLE_TRACKER,
    FEED_TRIP_UPDATES,
    MODE_METRO,
    ROUTE_TYPE_MAP,
)
from .realtime.trip_updates import parse_trip_updates
from .routing.planner import StopNotFoundError, cancelled_trips, plan_trip, resolve_station

_LOGGER = logging.getLogger(__name__)

SERVICE_PLAN_TRIP = "plan_trip"

ATTR_FROM = "from"
ATTR_TO = "to"
ATTR_DEPART_AFTER = "depart_after"
ATTR_MIN_TRANSFER = "min_transfer_minutes"
ATTR_RESULTS = "results"

# Live delays are only meaningful for trips leaving soon
_REALTIME_HORIZON = timedelta(hours=2)

PLAN_TRIP_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_FROM): cv.string,
        vol.Required(ATTR_TO): cv.string,
        vol.Optional(ATTR_DEPART_AFTER): vol.Any(cv.datetime, cv.time),
        vol.Optional(ATTR_MIN_TRANSFER, default=3): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=30)
        ),
        vol.Optional(ATTR_RESULTS, default=3): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=10)
        ),
    }
)


def _watch_entries(hass: HomeAssistant) -> list:
    return [
        e
        for e in hass.config_entries.async_entries(DOMAIN)
        if e.data.get(CONF_ENTRY_TYPE, "watch")
        not in (ENTRY_TYPE_VEHICLE_TRACKER, ENTRY_TYPE_ROUTE_TRACKER)
    ]


def _resolve_when(value: datetime | time | None) -> datetime:
    now = dt_util.now()
    if value is None:
        return now
    if isinstance(value, time):
        when = now.replace(
            hour=value.hour, minute=value.minute, second=value.second, microsecond=0
        )
        # A time that has already passed today means tomorrow
        return when if when >= now - timedelta(minutes=1) else when + timedelta(days=1)
    if value.tzinfo is None:
        return value.replace(tzinfo=dt_util.get_default_time_zone())
    return dt_util.as_local(value)


async def _live_delays(hass: HomeAssistant, entries: list) -> dict:
    """Metro trip-update delays from any loaded watch that has a realtime client."""
    for entry in entries:
        if entry.state is not ConfigEntryState.LOADED:
            continue
        client = getattr(getattr(entry, "runtime_data", None), "realtime_client", None)
        if client is None:
            continue
        try:
            feed = await client.fetch_feed(MODE_METRO, FEED_TRIP_UPDATES)
        except Exception as exc:  # noqa: BLE001 - realtime is best effort
            _LOGGER.warning("Trip planner: realtime fetch failed (%s); using timetable", exc)
            return {}
        return await hass.async_add_executor_job(parse_trip_updates, feed)
    return {}


def async_register_services(hass: HomeAssistant, db_path: Path) -> None:
    """Register the integration's actions."""

    async def handle_plan_trip(call: ServiceCall) -> ServiceResponse:
        entries = _watch_entries(hass)
        route_types: list[int] = []
        for entry in entries:
            for rt in ROUTE_TYPE_MAP.get(entry.data.get(CONF_MODE, ""), []):
                if rt not in route_types:
                    route_types.append(rt)
        if not route_types:
            route_types = list(ROUTE_TYPE_MAP[MODE_METRO])

        origin_query = call.data.get(ATTR_FROM)
        if not origin_query:
            if not entries:
                raise ServiceValidationError(
                    "No 'from' station given and no Transport Victoria watch to default to"
                )
            origin_query = entries[0].data.get(CONF_STOP_ID, "")

        try:
            origin = await hass.async_add_executor_job(
                resolve_station, db_path, origin_query, route_types
            )
            destination = await hass.async_add_executor_job(
                resolve_station, db_path, call.data[ATTR_TO], route_types
            )
        except StopNotFoundError as exc:
            raise ServiceValidationError(str(exc)) from exc

        when = _resolve_when(call.data.get(ATTR_DEPART_AFTER))
        delays: dict = {}
        if when - dt_util.now() < _REALTIME_HORIZON:
            delays = await _live_delays(hass, entries)

        local_naive = when.astimezone(dt_util.get_default_time_zone()).replace(tzinfo=None)
        options = await hass.async_add_executor_job(
            lambda: plan_trip(
                db_path,
                origin,
                destination,
                local_naive,
                trip_delays=delays,
                cancelled_trip_ids=cancelled_trips(delays),
                min_transfer_minutes=call.data[ATTR_MIN_TRANSFER],
                max_results=call.data[ATTR_RESULTS],
                tz=dt_util.get_default_time_zone(),
            )
        )

        results = [o.as_dict() for o in options]
        best = results[0] if results else None
        return {
            "from": origin.name,
            "to": destination.name,
            "depart_after": when.isoformat(),
            "realtime_used": bool(delays),
            "best": best,
            "options": results,
            "message": _message(origin.name, destination.name, best),
        }

    hass.services.async_register(
        DOMAIN,
        SERVICE_PLAN_TRIP,
        handle_plan_trip,
        schema=PLAN_TRIP_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )


def _message(origin: str, destination: str, best: dict | None) -> str:
    """One-line human summary of the best option."""
    if best is None:
        return f"No trains found from {origin} to {destination} in the next few hours."
    first = best["legs"][0]
    dep = first["depart"][11:16]
    arr = best["arrive"][11:16]
    if best["change_at"]:
        second = best["legs"][1]
        how = f"change at {best['change_at']}"
        if best["walk_to"]:
            how += f" and walk to {best['walk_to']}"
        return (
            f"{dep} {first['route']} from platform {first['platform'] or '?'}, {how} "
            f"for the {second['depart'][11:16]} {second['route']} train, arrive {arr} "
            f"({best['duration_minutes']} min)."
        )
    return (
        f"{dep} {first['route']} from platform {first['platform'] or '?'}, direct, "
        f"arrive {arr} ({best['duration_minutes']} min)."
    )
