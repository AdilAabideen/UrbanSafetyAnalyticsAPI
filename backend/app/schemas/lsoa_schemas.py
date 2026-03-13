from typing import List, Optional

from pydantic import BaseModel


class LsoaCategoryItem(BaseModel):
    lsoa_code: str
    lsoa_name: str
    count: int
    minLon: Optional[float] = None
    minLat: Optional[float] = None
    maxLon: Optional[float] = None
    maxLat: Optional[float] = None


class LsoaCategoriesResponse(BaseModel):
    items: List[LsoaCategoryItem]

