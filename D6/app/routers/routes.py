from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import BookingClass, ItineraryOut
from app.crud.routes import search_routes

router = APIRouter(tags=["routes"])


@router.get("/routes/search", response_model=list[ItineraryOut])
async def routes_search(
        fromAirportCode: str | None = Query(None),
        fromCity: str | None = Query(None),
        toAirportCode: str | None = Query(None),
        toCity: str | None = Query(None),
        departureDate: date = Query(...),
        bookingClass: BookingClass | None = Query(None),
        maxConnections: str | int | None = Query(3),
        session: AsyncSession = Depends(get_session),
):
    if maxConnections is None:
        max_conn_int = 3
    elif isinstance(maxConnections, str) and maxConnections.lower() == "unbound":
        max_conn_int = 10
    else:
        max_conn_int = int(maxConnections)
        if max_conn_int < 0:
            max_conn_int = 0
        if max_conn_int > 10:
            max_conn_int = 10

    return await search_routes(
        session=session,
        from_airport=fromAirportCode,
        from_city=fromCity,
        to_airport=toAirportCode,
        to_city=toCity,
        departure_date=departureDate,
        max_connections=max_conn_int,
        booking_class=bookingClass,
    )
