import uuid
from sqlmodel import SQLModel, Field, String
from datetime import datetime
from typing import Optional

class SpotPrice(SQLModel, table=True):
    ID: Optional[str] = Field(default_factory=uuid.uuid4, primary_key=True, max_length=100)
    HourUTC: datetime
    HourDK: datetime
    PriceArea: Optional[str] = Field(max_length=10)
    SpotPriceDKK: Optional[float]
    SpotPriceEUR: Optional[float]