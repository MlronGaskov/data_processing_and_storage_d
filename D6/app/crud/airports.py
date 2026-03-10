from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

SQL_CITIES = text("""
WITH params AS (
    SELECT (SELECT MAX(actual_departure) FROM flights) AS now_ts
)
select distinct
    city->>'en' as city,
    country->>'en' as country
from airports_data
where airport_code in (
    select departure_airport
    from routes
    where (select now_ts from params) <@ validity
)
or airport_code in (
    select arrival_airport
    from routes
    where (select now_ts from params) <@ validity
)
order by country, city;
""")

SQL_AIRPORTS = text("""
WITH params AS (
    SELECT (SELECT MAX(actual_departure) FROM flights) AS now_ts
)
select airport_code,
       airport_name->>'en' as airport,
       city->>'en' as city,
       country->>'en' as country
from airports_data
where airport_code in (
    select departure_airport from routes where (select now_ts from params) <@ validity
)
or airport_code in (
    select arrival_airport from routes where (select now_ts from params) <@ validity
)
order by country, city, airport;
""")

SQL_CITY_AIRPORTS_BASE = """
select airport_code,
       airport_name->>'en' as airport,
       city->>'en' as city,
       country->>'en' as country
from airports_data
where city->>'en' = :city
"""

SQL_CITY_AIRPORTS_ORDER = " order by country, city, airport;"

SQL_INBOUND = text("""
WITH params AS (
    SELECT (SELECT MAX(actual_departure) FROM flights) AS now_ts
)
select r.route_no,
       r.days_of_week,
       (((DATE '2000-01-15'::timestamp + r.scheduled_time)
            AT TIME ZONE origin.timezone
            + r.duration
        ) AT TIME ZONE destination.timezone)::time AS arrival_time,
       origin.airport_code as origin_airport,
       origin.airport_name->>'en' as origin_name,
       origin.city->>'en' as origin_city,
       origin.country->>'en' as origin_country
from routes r
    inner join airports_data origin on r.departure_airport = origin.airport_code
    inner join airports_data destination on r.arrival_airport = destination.airport_code
where (select now_ts from params) <@ r.validity
  and r.arrival_airport = :arrival_airport
order by r.route_no;
""")

SQL_OUTBOUND = text("""
WITH params AS (
    SELECT (SELECT MAX(actual_departure) FROM flights) AS now_ts
)
select r.route_no,
       r.days_of_week,
       r.scheduled_time,
       destination.airport_code as destination_airport,
       destination.airport_name->>'en' as destination_name,
       destination.city->>'en' as destination_city,
       destination.country->>'en' as destination_country
from routes r
    inner join airports_data destination on r.arrival_airport = destination.airport_code
where (select now_ts from params) <@ r.validity
  and r.departure_airport = :departure_airport
order by r.route_no;
""")


async def list_cities(session: AsyncSession):
    rows = (await session.execute(SQL_CITIES)).mappings().all()
    return [{"city": r["city"], "country": r["country"]} for r in rows]


async def list_airports(session: AsyncSession):
    rows = (await session.execute(SQL_AIRPORTS)).mappings().all()
    return [
        {
            "airportCode": r["airport_code"],
            "airportName": r["airport"],
            "city": r["city"],
            "country": r["country"],
        }
        for r in rows
    ]


async def airports_in_city(session: AsyncSession, city: str, country: str | None):
    sql = SQL_CITY_AIRPORTS_BASE
    params = {"city": city}

    if country is not None:
        sql += " and country->>'en' = :country"
        params["country"] = country

    sql += SQL_CITY_AIRPORTS_ORDER

    rows = (await session.execute(text(sql), params)).mappings().all()
    return [
        {
            "airportCode": r["airport_code"],
            "airportName": r["airport"],
            "city": r["city"],
            "country": r["country"],
        }
        for r in rows
    ]


async def inbound_schedule(session: AsyncSession, airport_code: str):
    rows = (await session.execute(SQL_INBOUND, {"arrival_airport": airport_code})).mappings().all()
    return [
        {
            "routeNo": r["route_no"],
            "daysOfWeek": r["days_of_week"],
            "arrivalTime": str(r["arrival_time"]),
            "origin": {
                "airportCode": r["origin_airport"],
                "airportName": r["origin_name"],
                "city": r["origin_city"],
                "country": r["origin_country"],
            },
        }
        for r in rows
    ]


async def outbound_schedule(session: AsyncSession, airport_code: str):
    rows = (await session.execute(SQL_OUTBOUND, {"departure_airport": airport_code})).mappings().all()
    return [
        {
            "routeNo": r["route_no"],
            "daysOfWeek": r["days_of_week"],
            "departureTime": str(r["scheduled_time"]),
            "destination": {
                "airportCode": r["destination_airport"],
                "airportName": r["destination_name"],
                "city": r["destination_city"],
                "country": r["destination_country"],
            },
        }
        for r in rows
    ]
