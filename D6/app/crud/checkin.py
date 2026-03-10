from __future__ import annotations

from datetime import timedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def check_in(session: AsyncSession, ticket_no: str):
    existing = (
        await session.execute(
            text("""
                select bp.flight_id,
                       bp.seat_no,
                       bp.boarding_no,
                       bp.boarding_time
                from boarding_passes bp
                where bp.ticket_no = :ticket_no
                order by bp.flight_id
            """),
            {"ticket_no": ticket_no},
        )
    ).mappings().all()

    if existing:
        return [
            {
                "flightId": int(r["flight_id"]),
                "seatNo": r["seat_no"],
                "boardingNo": int(r["boarding_no"]),
                "boardingTime": r["boarding_time"],
            }
            for r in existing
        ]

    seg_rows = (
        await session.execute(
            text("""
                select s.flight_id,
                       s.fare_conditions
                from segments s
                where s.ticket_no = :ticket_no
                order by s.flight_id
            """),
            {"ticket_no": ticket_no},
        )
    ).mappings().all()

    if not seg_rows:
        return []

    results = []

    for seg in seg_rows:
        flight_id = int(seg["flight_id"])
        fare_conditions = seg["fare_conditions"]

        flight_row = (
            await session.execute(
                text("""
                    select f.scheduled_departure,
                           r.airplane_code
                    from flights f
                    join routes r
                      on r.route_no = f.route_no
                     and f.scheduled_departure <@ r.validity
                    where f.flight_id = :flight_id
                    limit 1
                """),
                {"flight_id": flight_id},
            )
        ).mappings().first()

        if not flight_row:
            continue

        scheduled_departure = flight_row["scheduled_departure"]
        airplane_code = flight_row["airplane_code"]

        seat_row = (
            await session.execute(
                text("""
                    select st.seat_no
                    from seats st
                    where st.airplane_code = :airplane_code
                      and st.fare_conditions = :fare_conditions
                      and not exists (
                          select 1
                          from boarding_passes bp
                          where bp.flight_id = :flight_id
                            and bp.seat_no = st.seat_no
                      )
                    order by st.seat_no
                    limit 1
                """),
                {
                    "airplane_code": airplane_code,
                    "fare_conditions": fare_conditions,
                    "flight_id": flight_id,
                },
            )
        ).mappings().first()

        if not seat_row:
            continue

        seat_no = seat_row["seat_no"]
        boarding_time = scheduled_departure - timedelta(minutes=30)

        next_no_row = (
            await session.execute(
                text("""
                    select coalesce(max(bp.boarding_no), 0) + 1 as next_no
                    from boarding_passes bp
                    where bp.flight_id = :flight_id
                """),
                {"flight_id": flight_id},
            )
        ).mappings().first()

        boarding_no = int(next_no_row["next_no"]) if next_no_row else 1

        await session.execute(
            text("""
                insert into boarding_passes (
                    ticket_no,
                    flight_id,
                    seat_no,
                    boarding_no,
                    boarding_time
                )
                values (
                    :ticket_no,
                    :flight_id,
                    :seat_no,
                    :boarding_no,
                    :boarding_time
                )
            """),
            {
                "ticket_no": ticket_no,
                "flight_id": flight_id,
                "seat_no": seat_no,
                "boarding_no": boarding_no,
                "boarding_time": boarding_time,
            },
        )

        results.append(
            {
                "flightId": flight_id,
                "seatNo": seat_no,
                "boardingNo": boarding_no,
                "boardingTime": boarding_time,
            }
        )

    await session.commit()
    return results