"""Trip planner: best way between two stations, direct or with one change.

Works on the imported GTFS schedule (SQLite) and, when available, realtime
trip-update delays. For every station where a change is possible it finds the
connection that arrives at the destination earliest, so callers can show which
station to change at.

Runs synchronously; call it from an executor.
"""

from __future__ import annotations

import bisect
import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, tzinfo
from pathlib import Path

from ..gtfs.time_utils import (
    day_of_week_column,
    departure_seconds_to_datetime,
    service_date_str,
    service_day_for_time,
)
from ..realtime.trip_updates import StopDelay, effective_delay, lookup_stop_delay

_LOGGER = logging.getLogger(__name__)

# Search windows (seconds after the requested departure time)
FIRST_LEG_WINDOW = 90 * 60
SECOND_LEG_WINDOW = 4 * 3600
DIRECT_WINDOW = 3 * 3600

_CHUNK = 500

# Walking changes between nearby stations (e.g. Melbourne Central <-> State Library)
MAX_WALK_METRES = 450
WALK_SPEED_MPS = 1.1
WALK_EXTRA_SECONDS = 60


class StopNotFoundError(Exception):
    """Raised when a stop name or id cannot be resolved."""


@dataclass(slots=True)
class Station:
    """A station and all of its platform stop_ids."""

    key: str
    name: str
    stop_ids: list[str]


@dataclass(slots=True)
class Leg:
    """One train leg of a trip."""

    trip_id: str
    route_name: str
    headsign: str
    from_name: str
    from_platform: str | None
    to_name: str
    depart: datetime
    arrive: datetime
    scheduled_depart: datetime
    scheduled_arrive: datetime
    realtime: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "route": self.route_name,
            "headsign": self.headsign,
            "from": self.from_name,
            "platform": self.from_platform,
            "to": self.to_name,
            "depart": self.depart.isoformat(),
            "arrive": self.arrive.isoformat(),
            "scheduled_depart": self.scheduled_depart.isoformat(),
            "scheduled_arrive": self.scheduled_arrive.isoformat(),
            "delay_minutes": round(
                (self.depart - self.scheduled_depart).total_seconds() / 60
            ),
            "realtime": self.realtime,
        }


@dataclass(slots=True)
class TripOption:
    """A complete way to make the trip."""

    legs: list[Leg] = field(default_factory=list)

    @property
    def depart(self) -> datetime:
        return self.legs[0].depart

    @property
    def arrive(self) -> datetime:
        return self.legs[-1].arrive

    @property
    def change_at(self) -> str | None:
        return self.legs[0].to_name if len(self.legs) > 1 else None

    @property
    def walk_to(self) -> str | None:
        if len(self.legs) > 1 and self.legs[1].from_name != self.legs[0].to_name:
            return self.legs[1].from_name
        return None

    def as_dict(self) -> dict[str, object]:
        change_at = self.change_at
        wait = None
        if len(self.legs) > 1:
            wait = round((self.legs[1].depart - self.legs[0].arrive).total_seconds() / 60)
        walk_to = self.walk_to
        if not change_at:
            summary = "Direct"
        elif walk_to:
            summary = f"Change at {change_at}, walk to {walk_to}"
        else:
            summary = f"Change at {change_at}"
        return {
            "summary": summary,
            "change_at": change_at,
            "walk_to": walk_to,
            "transfer_wait_minutes": wait,
            "depart": self.depart.isoformat(),
            "arrive": self.arrive.isoformat(),
            "duration_minutes": round((self.arrive - self.depart).total_seconds() / 60),
            "realtime": all(leg.realtime for leg in self.legs),
            "legs": [leg.as_dict() for leg in self.legs],
        }


# ---------------------------------------------------------------------------
# Station lookup
# ---------------------------------------------------------------------------


def resolve_station(db_path: Path, query: str, route_types: list[int]) -> Station:
    """Resolve a stop name (or stop_id) to a station with all its platforms."""
    q = query.strip()
    if not q:
        raise StopNotFoundError("empty stop name")
    rt = ", ".join("?" * len(route_types))
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT stop_id, stop_name, parent_station FROM stops WHERE stop_id = ?", [q]
        ).fetchone()
        if row is None:
            base = q.lower().removesuffix(" station").strip()
            rows = conn.execute(
                f"""
                SELECT s.stop_id, s.stop_name, s.parent_station
                FROM stops s
                WHERE (s.location_type = 0 OR s.location_type IS NULL)
                  AND lower(s.stop_name) LIKE ?
                  AND EXISTS (
                      SELECT 1 FROM stop_times st
                      JOIN trips t ON t.trip_id = st.trip_id
                      JOIN routes r ON r.route_id = t.route_id
                      WHERE st.stop_id = s.stop_id AND r.route_type IN ({rt})
                  )
                ORDER BY s.stop_name
                """,
                [f"%{base}%", *route_types],
            ).fetchall()
            if not rows:
                raise StopNotFoundError(f"no station matches '{query}'")

            def rank(r: sqlite3.Row) -> tuple[int, int, str]:
                name = r["stop_name"].lower()
                plain = name.removesuffix(" station").strip()
                return (
                    0 if plain == base else 1 if name.startswith(base) else 2,
                    len(name),
                    name,
                )

            row = min(rows, key=rank)

        parent = row["parent_station"]
        if parent:
            members = [
                r[0]
                for r in conn.execute(
                    "SELECT stop_id FROM stops WHERE parent_station = ?"
                    " AND (location_type = 0 OR location_type IS NULL)",
                    [parent],
                )
            ]
            return Station(key=parent, name=row["stop_name"], stop_ids=members or [row["stop_id"]])
        return Station(key=row["stop_id"], name=row["stop_name"], stop_ids=[row["stop_id"]])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Call:
    """One scheduled call of a trip at a stop, with live times applied."""

    trip_id: str
    stop_id: str
    seq: int
    arr: int  # live arrival, seconds from service-day midnight
    dep: int  # live departure
    sched_arr: int
    sched_dep: int
    realtime: bool


def _active_services(conn: sqlite3.Connection, service_date: date) -> set[str]:
    dow = day_of_week_column(service_date)
    ds = service_date_str(service_date)
    rows = conn.execute(
        f"""
        SELECT service_id FROM calendar
        WHERE start_date <= :d AND end_date >= :d AND {dow} = 1
        UNION
        SELECT service_id FROM calendar_dates WHERE date = :d AND exception_type = 1
        EXCEPT
        SELECT service_id FROM calendar_dates WHERE date = :d AND exception_type = 2
        """,
        {"d": ds},
    ).fetchall()
    return {r[0] for r in rows}


def _chunks(items: list[str]) -> list[list[str]]:
    return [items[i : i + _CHUNK] for i in range(0, len(items), _CHUNK)]


class _Live:
    """Applies realtime delays to scheduled seconds."""

    def __init__(
        self,
        service_date: date,
        trip_delays: dict[str, StopDelay],
        cancelled: set[str],
    ) -> None:
        self._date = service_date
        self._delays = trip_delays
        self.cancelled = cancelled

    def adjust(self, trip_id: str, stop_id: str, seq: int, secs: int) -> tuple[int, bool]:
        sd = lookup_stop_delay(self._delays, trip_id, stop_id, seq)
        if sd is None:
            return secs, False
        sched = departure_seconds_to_datetime(self._date, secs)
        return secs + effective_delay(sd, sched), True


def _trips_meta(conn: sqlite3.Connection, trip_ids: list[str]) -> dict[str, tuple[str, str, str]]:
    meta: dict[str, tuple[str, str, str]] = {}
    for chunk in _chunks(trip_ids):
        ph = ", ".join("?" * len(chunk))
        for r in conn.execute(
            f"""
            SELECT t.trip_id, t.service_id, COALESCE(r.route_short_name, r.route_long_name, ''),
                   COALESCE(t.trip_headsign, '')
            FROM trips t JOIN routes r ON r.route_id = t.route_id
            WHERE t.trip_id IN ({ph})
            """,
            chunk,
        ):
            meta[r[0]] = (r[1], r[2], r[3])
    return meta


def _calls_for_trips(
    conn: sqlite3.Connection, trip_ids: list[str], live: _Live
) -> dict[str, list[_Call]]:
    """All calls of the given trips, ordered by stop_sequence, with live times."""
    out: dict[str, list[_Call]] = {}
    for chunk in _chunks(trip_ids):
        ph = ", ".join("?" * len(chunk))
        for r in conn.execute(
            f"""
            SELECT trip_id, stop_id, stop_sequence, arrival_seconds, departure_seconds,
                   pickup_type, drop_off_type
            FROM stop_times WHERE trip_id IN ({ph})
            ORDER BY trip_id, stop_sequence
            """,
            chunk,
        ):
            trip_id, stop_id, seq, arr, dep = r[0], r[1], r[2], r[3], r[4]
            arr = arr if arr is not None else dep
            dep = dep if dep is not None else arr
            if arr is None:
                continue
            live_arr, rt_a = live.adjust(trip_id, stop_id, seq, arr)
            live_dep, rt_d = live.adjust(trip_id, stop_id, seq, dep)
            call = _Call(trip_id, stop_id, seq, live_arr, live_dep, arr, dep, rt_a or rt_d)
            # Encode pickup/drop-off restrictions by nulling the side that is not allowed
            call_pickup_ok = (r[5] or 0) != 1
            call_dropoff_ok = (r[6] or 0) != 1
            if not call_pickup_ok:
                call.dep = -1
            if not call_dropoff_ok:
                call.arr = -1
            out.setdefault(trip_id, []).append(call)
    return out


def _trips_at(
    conn: sqlite3.Connection,
    stop_ids: list[str],
    lo: int,
    hi: int,
    services: set[str],
) -> list[str]:
    ph = ", ".join("?" * len(stop_ids))
    rows = conn.execute(
        f"""
        SELECT DISTINCT st.trip_id, t.service_id
        FROM stop_times st JOIN trips t ON t.trip_id = st.trip_id
        WHERE st.stop_id IN ({ph}) AND st.departure_seconds BETWEEN ? AND ?
        """,
        [*stop_ids, lo, hi],
    ).fetchall()
    return [r[0] for r in rows if r[1] in services]


def plan_trip(
    db_path: Path,
    origin: Station,
    destination: Station,
    now: datetime,
    trip_delays: dict[str, StopDelay] | None = None,
    cancelled_trip_ids: set[str] | None = None,
    min_transfer_minutes: int = 3,
    max_results: int = 3,
    tz: tzinfo | None = None,
) -> list[TripOption]:
    """Return the best trip options, earliest arrival first.

    Includes the best direct option (if any) and the best option through each
    station where a change works, so the list answers "where should I change?".
    """
    if origin.key == destination.key:
        return []
    service_date, t0 = service_day_for_time(now)
    min_xfer = max(0, min_transfer_minutes) * 60
    live = _Live(service_date, trip_delays or {}, cancelled_trip_ids or set())
    origin_ids = set(origin.stop_ids)
    dest_ids = set(destination.stop_ids)

    conn = sqlite3.connect(str(db_path))
    try:
        services = _active_services(conn, service_date)
        if not services:
            _LOGGER.warning("No active services on %s; is the timetable out of date?", service_date)
            return []

        first_trips = _trips_at(conn, origin.stop_ids, t0 - 1800, t0 + DIRECT_WINDOW, services)
        last_trips = _trips_at(
            conn, destination.stop_ids, t0, t0 + SECOND_LEG_WINDOW + 3600, services
        )
        trip_ids = sorted(set(first_trips) | set(last_trips))
        trip_ids = [t for t in trip_ids if not _is_cancelled(t, live.cancelled)]
        calls = _calls_for_trips(conn, trip_ids, live)
        meta = _trips_meta(conn, trip_ids)

        # Station key and name for every stop we touch
        stop_ids = sorted({c.stop_id for cs in calls.values() for c in cs})
        station_of: dict[str, str] = {}
        name_of: dict[str, str] = {}
        platform_of: dict[str, str | None] = {}
        coord_of: dict[str, tuple[float, float]] = {}
        for chunk in _chunks(stop_ids):
            ph = ", ".join("?" * len(chunk))
            for r in conn.execute(
                f"""
                SELECT s.stop_id, COALESCE(s.parent_station, s.stop_id),
                       s.stop_name, s.platform_code, s.stop_lat, s.stop_lon
                FROM stops s
                WHERE s.stop_id IN ({ph})
                """,
                chunk,
            ):
                station_of[r[0]] = r[1]
                name_of.setdefault(r[1], r[2])
                platform_of[r[0]] = r[3] or None
                if r[4] is not None and r[5] is not None:
                    coord_of.setdefault(r[1], (r[4], r[5]))
    finally:
        conn.close()

    def dt(secs: int) -> datetime:
        value = departure_seconds_to_datetime(service_date, secs)
        return value.replace(tzinfo=tz) if tz else value

    def make_leg(trip_id: str, board: _Call, alight: _Call) -> Leg:
        _svc, route, headsign = meta.get(trip_id, ("", "", ""))
        return Leg(
            trip_id=trip_id,
            route_name=route,
            headsign=headsign,
            from_name=name_of.get(station_of.get(board.stop_id, ""), board.stop_id),
            from_platform=platform_of.get(board.stop_id),
            to_name=name_of.get(station_of.get(alight.stop_id, ""), alight.stop_id),
            depart=dt(board.dep),
            arrive=dt(alight.arr),
            scheduled_depart=dt(board.sched_dep),
            scheduled_arrive=dt(alight.sched_arr),
            realtime=board.realtime and alight.realtime,
        )

    # ---- Leg 1 candidates: boarding at the origin at or after t0 ----
    boardings: list[tuple[str, int]] = []  # (trip_id, index of boarding call)
    for trip_id in first_trips:
        cs = calls.get(trip_id, [])
        for i, c in enumerate(cs):
            if c.stop_id in origin_ids and c.dep >= t0 and c.dep <= t0 + DIRECT_WINDOW:
                boardings.append((trip_id, i))
                break

    options: list[TripOption] = []

    # ---- Direct ----
    best_direct: TripOption | None = None
    for trip_id, bi in boardings:
        cs = calls[trip_id]
        for c in cs[bi + 1 :]:
            if c.stop_id in dest_ids and c.arr >= 0:
                opt = TripOption([make_leg(trip_id, cs[bi], c)])
                if best_direct is None or _better(opt, best_direct):
                    best_direct = opt
                break
    if best_direct:
        options.append(best_direct)

    # ---- One change ----
    # Second-leg departures per station: sorted (dep, arrival_at_dest, trip, board_idx, alight_idx)
    second: dict[str, list[tuple[int, int, str, int, int]]] = {}
    for trip_id in last_trips:
        cs = calls.get(trip_id, [])
        alight_idx = next(
            (i for i, c in enumerate(cs) if c.stop_id in dest_ids and c.arr >= 0), None
        )
        if alight_idx is None:
            continue
        arr_dest = cs[alight_idx].arr
        for i in range(alight_idx):
            c = cs[i]
            if c.dep < t0:
                continue
            st = station_of.get(c.stop_id, c.stop_id)
            if st in (origin.key, destination.key):
                continue
            second.setdefault(st, []).append((c.dep, arr_dest, trip_id, i, alight_idx))
    for lst in second.values():
        lst.sort()

    # Stations close enough to walk between, with walking time in seconds
    walk: dict[str, list[tuple[str, int]]] = {}
    first_stations = {
        station_of.get(c.stop_id, c.stop_id)
        for t, _ in boardings
        for c in calls.get(t, [])
    }
    for a in first_stations:
        if a not in coord_of:
            continue
        for b in second:
            if b == a or b not in coord_of:
                continue
            metres = _haversine_m(*coord_of[a], *coord_of[b])
            if metres <= MAX_WALK_METRES:
                walk.setdefault(a, []).append(
                    (b, int(metres / WALK_SPEED_MPS) + WALK_EXTRA_SECONDS)
                )

    best_by_station: dict[tuple[str, str], TripOption] = {}
    for trip_id, bi in boardings:
        if trip_id not in calls:
            continue
        cs = calls[trip_id]
        board = cs[bi]
        if board.dep > t0 + FIRST_LEG_WINDOW:
            continue
        for c in cs[bi + 1 :]:
            if c.arr < 0:
                continue
            st = station_of.get(c.stop_id, c.stop_id)
            if st in (origin.key, destination.key):
                continue
            targets = [(st, min_xfer)] + [(b, max(min_xfer, w)) for b, w in walk.get(st, [])]
            for target, xfer in targets:
                if target not in second or target in (origin.key, destination.key):
                    continue
                lst = second[target]
                k = bisect.bisect_left(lst, (c.arr + xfer,))
                pick: tuple[int, int, str, int, int] | None = None
                for cand in lst[k:]:
                    if cand[2] == trip_id:
                        continue
                    if pick is None or cand[1] < pick[1]:
                        pick = cand
                if pick is None:
                    continue
                _dep2, _arr, trip2, b2, a2 = pick
                cs2 = calls[trip2]
                opt = TripOption(
                    [make_leg(trip_id, board, c), make_leg(trip2, cs2[b2], cs2[a2])]
                )
                cur = best_by_station.get((st, target))
                if cur is None or _better(opt, cur):
                    best_by_station[(st, target)] = opt

    # A change is only worth showing if it beats going direct
    for opt in best_by_station.values():
        if best_direct is None or opt.arrive < best_direct.arrive:
            options.append(opt)

    options.sort(key=lambda o: (o.arrive, -o.depart.timestamp(), len(o.legs)))
    return options[: max(1, max_results)]


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _better(a: TripOption, b: TripOption) -> bool:
    """Earlier arrival wins; then later departure; then fewer legs."""
    if a.arrive != b.arrive:
        return a.arrive < b.arrive
    if a.depart != b.depart:
        return a.depart > b.depart
    return len(a.legs) < len(b.legs)


def _is_cancelled(trip_id: str, cancelled: set[str]) -> bool:
    if not cancelled:
        return False
    from ..realtime.trip_updates import normalize_trip_id

    return normalize_trip_id(trip_id) in cancelled


def cancelled_trips(trip_delays: dict[str, StopDelay]) -> set[str]:
    """Normalised trip_ids the realtime feed marks as cancelled."""
    out: set[str] = set()
    for key, sd in trip_delays.items():
        if sd.is_cancelled:
            out.add(key.split(":", 1)[0].split("#", 1)[0])
    return out


def window_end(now: datetime) -> datetime:
    """Latest departure the planner considers, for messages."""
    return now + timedelta(seconds=DIRECT_WINDOW)
