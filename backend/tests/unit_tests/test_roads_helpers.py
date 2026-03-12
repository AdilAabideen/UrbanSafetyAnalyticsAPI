import pytest
from fastapi import HTTPException

from app.api import roads


def test_validate_bbox_accepts_valid_bounds():
    roads._validate_bbox(-1.6, 53.78, -1.5, 53.82)


def test_validate_bbox_rejects_reversed_longitudes():
    with pytest.raises(HTTPException) as exc:
        roads._validate_bbox(-1.5, 53.78, -1.6, 53.82)

    assert exc.value.status_code == 400
    assert exc.value.detail == "minLon must be less than maxLon"


def test_validate_tile_coordinates_rejects_out_of_range_values():
    with pytest.raises(HTTPException) as exc:
        roads._validate_tile_coordinates(9, 512, 0)

    assert exc.value.status_code == 400
    assert exc.value.detail == "Tile coordinates out of range for zoom level"


def test_tile_profile_changes_across_zoom_bands():
    low_zoom_highways, low_zoom_tolerance = roads._tile_profile(8)
    high_zoom_highways, high_zoom_tolerance = roads._tile_profile(14)

    assert "primary" in low_zoom_highways
    assert low_zoom_tolerance == 80
    assert high_zoom_highways is None
    assert high_zoom_tolerance == 0


def test_parse_json_handles_string_and_dict_values():
    payload = {"ok": True}

    assert roads._parse_json('{"ok": true}') == payload
    assert roads._parse_json(payload) == payload
