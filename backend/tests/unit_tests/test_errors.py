from fastapi import HTTPException

from app.errors import (
    AppError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    DependencyError,
    ErrorResponse,
    InternalAppError,
    NotFoundError,
    ValidationError,
    http_exception_to_app_error,
)


def test_app_error_subclasses_have_expected_defaults():
    v = ValidationError()
    assert v.status_code == 400
    assert v.error == "INVALID_REQUEST"

    a = AuthenticationError()
    assert a.status_code == 401
    assert a.error == "NOT_AUTHENTICATED"

    authz = AuthorizationError()
    assert authz.status_code == 403
    assert authz.error == "FORBIDDEN"

    nf = NotFoundError()
    assert nf.status_code == 404
    assert nf.error == "RESOURCE_NOT_FOUND"

    c = ConflictError()
    assert c.status_code == 409
    assert c.error == "CONFLICT"

    d = DependencyError()
    assert d.status_code == 503
    assert d.error == "DB_UNAVAILABLE"

    i = InternalAppError()
    assert i.status_code == 500
    assert i.error == "INTERNAL_SERVER_ERROR"


def test_http_exception_to_app_error_mapping():
    exc = HTTPException(status_code=400, detail="Bad thing")
    err = http_exception_to_app_error(exc)

    assert isinstance(err, ValidationError)
    assert err.status_code == 400
    assert err.error == "INVALID_REQUEST"
    assert err.message == "Bad thing"


def test_error_response_builds_standard_payload():
    resp = ErrorResponse(status_code=400, error="INVALID_REQUEST", message="oops", details={"field": "x"})

    assert resp.status_code == 400
    assert resp.body is not None

