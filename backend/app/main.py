from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.roads import router as roads_router


app = FastAPI(title="Urban Risk Analytics API")
app.include_router(roads_router)

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

