from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils import gen_book_ref, gen_ticket_no


@dataclass(frozen=True)
class SegmentKey:
    route_no: str
    dep_airport: str
    arr_airport: str
    dep_time: datetime


async def _resolve_flight_ids(session: AsyncSession, segments: Sequence[SegmentKey]) -> list[int]:
    flight_ids: list[int] = []
    for s in segments:
        row = (
            await session.execute(
                text("""
                    select f.flight_id
                    from flights f
                    join routes r on r.route_no = f.route_no
                    where f.route_no = :route_no
                      and r.departure_airport = :dep
                      and r.arrival_airport = :arr
                      and f.scheduled_departure = :dep_time
                    limit 1
                """),
                {
                    "route_no": s.route_no,
                    "dep": s.dep_airport,
                    "arr": s.arr_airport,
                    "dep_time": s.dep_time,
                },
            )
        ).mappings().first()

        if not row:
            raise ValueError(
                f"Cannot resolve flight_id for segment: routeNo={s.route_no}, "
                f"{s.dep_airport}->{s.arr_airport}, departureTime={s.dep_time.isoformat()}"
            )

        flight_ids.append(int(row["flight_id"]))

    return flight_ids


async def create_booking_from_segments(
        session: AsyncSession,
        passenger_id: str,
        passenger_name: str,
        segments,
        fare_condition: str = "Economy",
        outbound: bool = True,
) -> str:
    if not segments:
        raise ValueError("segments must be non-empty")

    seg_keys = [
        SegmentKey(
            route_no=s.routeNo,
            dep_airport=s.departureAirportCode,
            arr_airport=s.arrivalAirportCode,
            dep_time=s.departureTime,
        )
        for s in segments
    ]

    flight_ids = await _resolve_flight_ids(session, seg_keys)

    for _ in range(50):
        book_ref = gen_book_ref()
        exists = (await session.execute(
            text("select 1 from bookings where book_ref = :br limit 1"),
            {"br": book_ref},
        )).first()
        if not exists:
            break
    else:
        raise RuntimeError("Failed to generate unique book_ref")

    for _ in range(50):
        ticket_no = gen_ticket_no()
        exists = (await session.execute(
            text("select 1 from tickets where ticket_no = :t limit 1"),
            {"t": ticket_no},
        )).first()
        if not exists:
            break
    else:
        raise RuntimeError("Failed to generate unique ticket_no")

    book_date = datetime.now(timezone.utc)

    await session.execute(
        text("""
            INSERT INTO bookings(book_ref, book_date, total_amount)
            SELECT
                :book_ref,
                :book_date,
                COALESCE(SUM(
                    CASE :fare_condition
                        WHEN 'Business' THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 100
                        WHEN 'Comfort'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 65
                        WHEN 'Economy'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 50
                    END
                ), 0)
            FROM flights f
            WHERE f.flight_id = ANY(:flights)
        """),
        {
            "book_ref": book_ref,
            "book_date": book_date,
            "fare_condition": fare_condition,
            "flights": flight_ids,
        },
    )

    await session.execute(
        text("""
            INSERT INTO tickets(ticket_no, book_ref, passenger_id, passenger_name, outbound)
            VALUES (:ticket_no, :book_ref, :passenger_id, :passenger_name, :outbound)
        """),
        {
            "ticket_no": ticket_no,
            "book_ref": book_ref,
            "passenger_id": passenger_id,
            "passenger_name": passenger_name,
            "outbound": outbound,
        },
    )

    await session.execute(
        text("""
            INSERT INTO segments(ticket_no, flight_id, fare_conditions, price)
            SELECT
                :ticket_no,
                f.flight_id,
                :fare_condition,
                CASE :fare_condition
                    WHEN 'Business' THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 100
                    WHEN 'Comfort'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 65
                    WHEN 'Economy'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 50
                END
            FROM flights f
            WHERE f.flight_id = ANY(:flights)
        """),
        {
            "ticket_no": ticket_no,
            "fare_condition": fare_condition,
            "flights": flight_ids,
        },
    )

    await session.commit()
    return ticket_no
