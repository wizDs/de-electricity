import uuid
from sqlmodel import SQLModel, Field, String
from datetime import datetime
from typing import Optional

class ConsumptionPerGridArea(SQLModel, table=True):
    ID: Optional[str] = Field(default_factory=uuid.uuid4, primary_key=True, max_length=100)
    HourUTC: datetime
    HourDK: datetime
    GridCompany: int = Field(ge=0, le=999)
    ResidualConsumption: Optional[float]
    FlexSettledConsumption: Optional[float]
    HourlySettledConsumption: Optional[float]