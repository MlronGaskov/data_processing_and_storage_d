from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

MIN_CONNECTION_MINUTES = 40
MAX_CONNECTION_HOURS = 24


def _day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _normalize_booking_class(booking_class: str | None) -> str | None:
    if booking_class is None:
        return None

    value = booking_class.strip().lower()

    normalized = value
    if normalized is None:
        raise ValueError("bookingClass must be one of: economy, comfort, business, or None")

    return normalized


def _build_start_class_filter(param_name: str = "booking_class") -> str:
    return f"""
    EXISTS (
        SELECT 1
        FROM seats s
        WHERE s.airplane_code = r.airplane_code
          AND s.fare_conditions = :{param_name}
    )
    """


def _build_connection_class_filter(param_name: str = "booking_class") -> str:
    return f"""
    EXISTS (
        SELECT 1
        FROM seats s
        WHERE s.airplane_code = r2.airplane_code
          AND s.fare_conditions = :{param_name}
    )
    """


async def search_routes(
        session: AsyncSession,
        from_airport: str | None,
        from_city: str | None,
        to_airport: str | None,
        to_city: str | None,
        departure_date: date,
        max_connections: int,
        booking_class: str | None = None,
        min_connection_minutes: int = MIN_CONNECTION_MINUTES,
        max_connection_hours: int = MAX_CONNECTION_HOURS,
) -> list[dict[str, Any]]:
    if from_airport is None and from_city is None:
        return []

    if to_airport is None and to_city is None:
        return []

    if max_connections < 0:
        return []

    day_start, day_end = _day_bounds_utc(departure_date)
    normalized_class = _normalize_booking_class(booking_class)

    params: dict[str, Any] = {
        "day_start": day_start,
        "day_end": day_end,
        "max_connections": int(max_connections),
        "min_connection_interval": timedelta(minutes=int(min_connection_minutes)),
        "max_connection_interval": timedelta(hours=int(max_connection_hours)),
    }

    start_conditions: list[str] = [
        "f.scheduled_departure >= :day_start",
        "f.scheduled_departure < :day_end",
        "f.scheduled_departure <@ r.validity",
        "f.status = 'Scheduled'",
    ]

    if from_airport is not None:
        start_conditions.append("r.departure_airport = :departure_airport")
        params["departure_airport"] = from_airport

    if from_city is not None:
        start_conditions.append("dep_airport.city->>'en' = :departure_city")
        params["departure_city"] = from_city

    if normalized_class is not None:
        start_conditions.append(_build_start_class_filter())
        params["booking_class"] = normalized_class

    finish_conditions: list[str] = []

    if to_airport is not None:
        finish_conditions.append("arrival_airport = :arrival_airport")
        params["arrival_airport"] = to_airport

    if to_city is not None:
        finish_conditions.append("arrival_city = :arrival_city")
        params["arrival_city"] = to_city

    connection_conditions: list[str] = [
        "f2.scheduled_departure >= i.scheduled_arrival + :min_connection_interval",
        "f2.scheduled_departure <= i.scheduled_arrival + :max_connection_interval",
        "NOT (arr_airport2.city->>'en' = ANY(i.path_cities))",
        "(i.connections + 1 <= :max_connections)",
        "f2.scheduled_departure <@ r2.validity",
        "f2.status = 'Scheduled'",
    ]

    if normalized_class is not None:
        connection_conditions.append(_build_connection_class_filter())

    sql = f"""
    WITH RECURSIVE itins AS (
        SELECT
            f.flight_id,
            r.departure_airport,
            dep_airport.city->>'en' AS departure_city,
            f.scheduled_departure,
            r.arrival_airport,
            arr_airport.city->>'en' AS arrival_city,
            f.scheduled_arrival,
            0 AS connections,
            ARRAY[dep_airport.city->>'en', arr_airport.city->>'en']::text[] AS path_cities,
            ARRAY[r.departure_airport::text, r.arrival_airport::text]::text[] AS path_airports,
            ARRAY[f.flight_id::bigint]::bigint[] AS path_flights
        FROM flights f
        JOIN routes r
            ON r.route_no = f.route_no
        JOIN airports_data dep_airport
            ON dep_airport.airport_code = r.departure_airport
        JOIN airports_data arr_airport
            ON arr_airport.airport_code = r.arrival_airport
        WHERE {" AND ".join(start_conditions)}

        UNION ALL

        SELECT
            f2.flight_id,
            i.departure_airport,
            i.departure_city,
            i.scheduled_departure,
            r2.arrival_airport,
            arr_airport2.city->>'en' AS arrival_city,
            f2.scheduled_arrival,
            i.connections + 1 AS connections,
            (i.path_cities || (arr_airport2.city->>'en'))::text[] AS path_cities,
            (i.path_airports || r2.arrival_airport::text)::text[] AS path_airports,
            (i.path_flights || f2.flight_id::bigint)::bigint[] AS path_flights
        FROM itins i
        JOIN routes r2
            ON r2.departure_airport = i.arrival_airport
        JOIN flights f2
            ON f2.route_no = r2.route_no
        JOIN airports_data arr_airport2
            ON arr_airport2.airport_code = r2.arrival_airport
        WHERE {" AND ".join(connection_conditions)}
    )

    SELECT
        connections,
        path_flights
    FROM itins
    WHERE {" AND ".join(finish_conditions)}
    ORDER BY connections, path_flights;
    """

    rows = (await session.execute(text(sql), params)).mappings().all()

    result: list[dict[str, Any]] = []

    for row in rows:
        flight_ids = list(row["path_flights"])
        if not flight_ids:
            continue

        details_sql = text("""
            SELECT DISTINCT ON (f.flight_id)
                f.flight_id,
                f.route_no,
                r.departure_airport,
                r.arrival_airport,
                f.scheduled_departure,
                f.scheduled_arrival
            FROM flights f
            JOIN routes r
              ON r.route_no = f.route_no
             AND f.scheduled_departure <@ r.validity
            WHERE f.flight_id = ANY(:ids)
            ORDER BY f.flight_id, array_position(:ids, f.flight_id)
        """)

        detail_rows = (
            await session.execute(details_sql, {"ids": flight_ids})
        ).mappings().all()

        segments = [
            {
                "routeNo": d["route_no"],
                "departureAirportCode": d["departure_airport"],
                "arrivalAirportCode": d["arrival_airport"],
                "departureTime": d["scheduled_departure"],
                "arrivalTime": d["scheduled_arrival"],
            }
            for d in detail_rows
        ]

        result.append(
            {
                "connectionsCount": int(row["connections"]),
                "segments": segments,
            }
        )

    return result
