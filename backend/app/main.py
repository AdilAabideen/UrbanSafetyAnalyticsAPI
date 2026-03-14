from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .api.analytics import router as analytics_router
from .api.auth import router as auth_router
from .api.report_events import router as report_events_router
from .api.tiles import router as tiles_router
from .api.watchlist import router as watchlist_router
from .api.watchlist_analytics import router as watchlist_analytics_router
from .bootstrap import initialize_database
from .errors import (
    AppError,
    ErrorResponse,
    InternalAppError,
    ValidationError,
    http_exception_to_app_error,
)


app = FastAPI(title="Urban Risk Analytics API")

app.include_router(tiles_router)
app.include_router(analytics_router)
app.include_router(auth_router)
app.include_router(report_events_router)
app.include_router(watchlist_router)
app.include_router(watchlist_analytics_router)

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


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError):
    """
    Convert AppError (and subclasses) into the standard JSON error payload.
    """
    response = ErrorResponse(
        status_code=exc.status_code,
        error=exc.error,
        message=exc.message,
        details=exc.details,
    )
    if exc.headers:
        for key, value in exc.headers.items():
            response.headers[key] = value
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    Normalize raw HTTPException instances into the standard error shape.
    """
    app_error = http_exception_to_app_error(exc)
    return await app_error_handler(request, app_error)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Normalize FastAPI request validation errors into 400 INVALID_REQUEST.
    """
    # Build a compact field error structure for clients.
    errors = []
    for err in exc.errors():
        loc = err.get("loc", [])
        field = ".".join(str(part) for part in loc if isinstance(part, (str, int)))
        errors.append(
            {
                "field": field,
                "message": err.get("msg"),
                "type": err.get("type"),
            }
        )

    app_error = ValidationError(
        error="INVALID_REQUEST",
        message="Request validation failed",
        details={"errors": errors},
    )
    return await app_error_handler(request, app_error)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """
    Catch-all handler that prevents leaking internal details to clients.
    """
    # Let FastAPI/uvicorn log the full traceback; keep the response generic.
    app_error = InternalAppError()
    return await app_error_handler(request, app_error)


@app.on_event("startup")
def startup_initialize_database():
    initialize_database()
