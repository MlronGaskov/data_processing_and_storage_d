from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import BookingIn, BookingOut
from app.crud.bookings import create_booking_from_segments

router = APIRouter(tags=["bookings"])


@router.post("/bookings", status_code=201, response_model=BookingOut)
async def post_booking(payload: BookingIn, session: AsyncSession = Depends(get_session)):
    try:
        ticket_no = await create_booking_from_segments(
            session=session,
            passenger_id=payload.passengerId,
            passenger_name=payload.passengerName,
            segments=payload.segments,
            fare_condition="Economy",
            outbound=True,
        )
        return {"ticketNo": ticket_no}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))