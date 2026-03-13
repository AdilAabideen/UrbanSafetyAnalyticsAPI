from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.analytics import router as analytics_router
from .api.auth import router as auth_router
from .api.collisions import router as collisions_router
from .api.crimes import router as crimes_router
from .api.lsoa import router as lsoa_router
from .api.report_events import router as report_events_router
from .api.roads import router as roads_router
from .api.tiles import router as tiles_router
from .api.user_events import router as user_events_router
from .api.watchlist import router as watchlist_router
from .bootstrap import initialize_database


app = FastAPI(title="Urban Risk Analytics API")
app.include_router(roads_router)
app.include_router(crimes_router)
app.include_router(collisions_router)
app.include_router(tiles_router)
app.include_router(user_events_router)
app.include_router(analytics_router)
app.include_router(auth_router)
app.include_router(lsoa_router)
app.include_router(report_events_router)
app.include_router(watchlist_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"name": "Urban Risk Analytics API", "ok": True}


@app.get("/health")
def health_check():
    return {"status": "Healthy", "ok": True}


@app.on_event("startup")
def startup_initialize_database():
    initialize_database()
