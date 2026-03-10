from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import BoardingPassOut, CheckInIn
from app.crud.checkin import check_in

router = APIRouter(tags=["checkin"])


@router.post("/check-in", status_code=201, response_model=list[BoardingPassOut])
async def post_checkin(payload: CheckInIn, session: AsyncSession = Depends(get_session)):
    return await check_in(session=session, ticket_no=payload.ticketNo)
