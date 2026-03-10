from fastapi import FastAPI

from app.routers.airports import router as airports_router
from app.routers.routes import router as routes_router
from app.routers.bookings import router as bookings_router
from app.routers.checkin import router as checkin_router

app = FastAPI(title="D6", version="1.0.0")

app.include_router(airports_router)
app.include_router(routes_router)
app.include_router(bookings_router)
app.include_router(checkin_router)
