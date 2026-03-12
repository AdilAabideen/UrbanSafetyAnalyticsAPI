from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.crimes import router as crimes_router
from .api.roads import router as roads_router
from .api.tiles import router as tiles_router


app = FastAPI(title="Urban Risk Analytics API")
app.include_router(roads_router)
app.include_router(crimes_router)
app.include_router(tiles_router)

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
