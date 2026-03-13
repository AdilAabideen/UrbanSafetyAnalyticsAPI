from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from .crime_utils import _execute
from ..db import get_db


router = APIRouter(prefix="/analytics", tags=["analytics"])


@