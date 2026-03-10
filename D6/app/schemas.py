from datetime import datetime
from enum import Enum
from typing import List, Union

from pydantic import BaseModel, Field


class CityOut(BaseModel):
    city: str
    country: str


class AirportOut(BaseModel):
    airportCode: str
    airportName: str
    city: str
    country: str


class InboundScheduleOut(BaseModel):
    routeNo: str
    daysOfWeek: List[int]
    arrivalTime: str
    origin: AirportOut


class OutboundScheduleOut(BaseModel):
    routeNo: str
    daysOfWeek: List[int]
    departureTime: str
    destination: AirportOut


class BookingClass(str, Enum):
    Economy = "Economy"
    Comfort = "Comfort"
    Business = "Business"


MaxConnections = Union[int, str]


class SegmentOut(BaseModel):
    routeNo: str
    departureAirportCode: str
    arrivalAirportCode: str
    departureTime: datetime
    arrivalTime: datetime


class ItineraryOut(BaseModel):
    connectionsCount: int = Field(..., ge=0)
    segments: List[SegmentOut]


class BookingOut(BaseModel):
    ticketNo: str


class BookingSegmentIn(BaseModel):
    routeNo: str
    departureAirportCode: str
    arrivalAirportCode: str
    departureTime: datetime
    arrivalTime: datetime


class BookingIn(BaseModel):
    passengerId: str
    passengerName: str
    segments: List[BookingSegmentIn]


class CheckInIn(BaseModel):
    ticketNo: str


class BoardingPassOut(BaseModel):
    flightId: int
    seatNo: str
    boardingNo: int
    boardingTime: datetime
