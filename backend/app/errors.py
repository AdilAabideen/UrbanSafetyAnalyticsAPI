from typing import Any, Dict, Optional

from fastapi import HTTPException
from fastapi.responses import JSONResponse


class ErrorResponse(JSONResponse):
    """
    Convenience subclass for error responses.

    This still behaves like a normal JSONResponse, but using it in type hints
    and OpenAPI metadata makes the intent clearer.
    """

    def __init__(self, status_code: int, error: str, message: str, details: Optional[Dict[str, Any]] = None):
        content: Dict[str, Any] = {"error": error, "message": message}
        if details is not None:
            content["details"] = details
        super().__init__(status_code=status_code, content=content)


class AppError(Exception):
    """
    Base application error used as the single abstraction at the API boundary.

    All API-visible errors should be represented as an AppError (or subclass),
    which the global exception handlers convert into the standard JSON payload:

        {
          "error": "<CODE>",
          "message": "<human-readable message>",
          "details": { ... }  # optional
        }
    """

    def __init__(
        self,
        status_code: int,
        error: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.message = message
        self.details = details or None
        self.headers = headers or None


class ValidationError(AppError):
    def __init__(self, error: str = "INVALID_REQUEST", message: str = "Request validation failed", details=None):
        super().__init__(status_code=400, error=error, message=message, details=details)


class AuthenticationError(AppError):
    def __init__(self, error: str = "NOT_AUTHENTICATED", message: str = "Authentication required", details=None, headers=None):
        headers = headers or {"WWW-Authenticate": "Bearer"}
        super().__init__(status_code=401, error=error, message=message, details=details, headers=headers)


class AuthorizationError(AppError):
    def __init__(self, error: str = "FORBIDDEN", message: str = "You do not have permission to perform this action", details=None):
        super().__init__(status_code=403, error=error, message=message, details=details)


class NotFoundError(AppError):
    def __init__(self, error: str = "RESOURCE_NOT_FOUND", message: str = "The requested resource was not found", details=None):
        super().__init__(status_code=404, error=error, message=message, details=details)


class ConflictError(AppError):
    def __init__(self, error: str = "CONFLICT", message: str = "The request could not be completed due to a conflict", details=None):
        super().__init__(status_code=409, error=error, message=message, details=details)


class DependencyError(AppError):
    def __init__(self, error: str = "DB_UNAVAILABLE", message: str = "Database unavailable", details=None):
        super().__init__(status_code=503, error=error, message=message, details=details)


class InternalAppError(AppError):
    def __init__(self, error: str = "INTERNAL_SERVER_ERROR", message: str = "An unexpected error occurred", details=None):
        super().__init__(status_code=500, error=error, message=message, details=details)


def http_exception_to_app_error(exc: HTTPException) -> AppError:
    """
    Normalize raw HTTPException instances into AppError so the external
    contract always uses the standard error shape.
    """
    # Derive a generic code from the status, with special-cases where helpful.
    status_to_code = {
        400: "INVALID_REQUEST",
        401: "NOT_AUTHENTICATED",
        403: "FORBIDDEN",
        404: "RESOURCE_NOT_FOUND",
        409: "CONFLICT",
        422: "INVALID_REQUEST",
    }

    error_code = status_to_code.get(exc.status_code, "INTERNAL_SERVER_ERROR")
    message = str(exc.detail) if exc.detail is not None else "Request failed"
    headers = getattr(exc, "headers", None)

    if exc.status_code == 401:
        return AuthenticationError(error=error_code, message=message, details=None, headers=headers)
    if exc.status_code == 403:
        return AuthorizationError(error=error_code, message=message, details=None)
    if exc.status_code == 404:
        return NotFoundError(error=error_code, message=message, details=None)
    if exc.status_code == 409:
        return ConflictError(error=error_code, message=message, details=None)
    if exc.status_code == 400 or exc.status_code == 422:
        return ValidationError(error=error_code, message=message, details=None)

    return InternalAppError(error=error_code, message=message)

