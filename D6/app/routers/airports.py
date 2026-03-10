from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import AirportOut, CityOut, InboundScheduleOut, OutboundScheduleOut
from app.crud.airports import (
    list_cities,
    list_airports,
    airports_in_city,
    inbound_schedule,
    outbound_schedule,
)

router = APIRouter(tags=["airports"])


@router.get("/cities", response_model=list[CityOut])
async def get_cities(session: AsyncSession = Depends(get_session)):
    return await list_cities(session)


@router.get("/airports", response_model=list[AirportOut])
async def get_airports(session: AsyncSession = Depends(get_session)):
    return await list_airports(session)


@router.get("/cities/airports", response_model=list[AirportOut])
async def get_city_airports(
        city: str = Query(...),
        country: str | None = Query(None),
        session: AsyncSession = Depends(get_session),
):
    return await airports_in_city(session, city=city, country=country)


@router.get("/airports/{airportCode}/schedule/inbound", response_model=list[InboundScheduleOut])
async def get_inbound_schedule(
        airportCode: str,
        session: AsyncSession = Depends(get_session),
):
    return await inbound_schedule(session, airport_code=airportCode)


@router.get("/airports/{airportCode}/schedule/outbound", response_model=list[OutboundScheduleOut])
async def get_outbound_schedule(
        airportCode: str,
        session: AsyncSession = Depends(get_session),
):
    return await outbound_schedule(session, airport_code=airportCode)
