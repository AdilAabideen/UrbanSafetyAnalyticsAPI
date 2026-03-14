import pytest

from app.errors import AuthenticationError
from app.services.auth_service import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)


def test_password_hashing_and_verification():
    raw_password = "UnitPass-123"
    password_hash = hash_password(raw_password)

    assert password_hash != raw_password
    assert verify_password(raw_password, password_hash)
    assert not verify_password("wrong-password", password_hash)


def test_jwt_token_creation_and_decoding():
    token = create_access_token(42)
    payload = decode_access_token(token)

    assert payload["sub"] == "42"
    assert "exp" in payload


def test_invalid_token_decode_is_rejected():
    with pytest.raises(AuthenticationError) as exc_info:
        decode_access_token("not-a-valid-jwt")

    assert exc_info.value.error == "INVALID_TOKEN"
