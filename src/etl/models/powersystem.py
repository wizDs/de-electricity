import uuid
from sqlmodel import SQLModel, Field, String
from datetime import datetime
from typing import Optional

class PowerSystem(SQLModel, table=True):
    ID: Optional[str] = Field(default_factory=uuid.uuid4, primary_key=True, max_length=36)
    Minutes1UTC: datetime
    Minutes1DK: datetime
    CO2Emission: Optional[float]
    ProductionGe100MW: Optional[float]
    ProductionLt100MW: Optional[float]
    SolarPower: Optional[float]
    OffshoreWindPower: Optional[float]
    OnshoreWindPower: Optional[float]
    Exchange_Sum: Optional[float]
    Exchange_DK1_DE: Optional[float]
    Exchange_DK1_NL: Optional[float]
    Exchange_DK1_NO: Optional[float]
    Exchange_DK1_SE: Optional[float]
    Exchange_DK1_DK2: Optional[float]
    Exchange_DK2_DE: Optional[float]
    Exchange_DK2_SE: Optional[float]
    Exchange_Bornholm_SE: Optional[float]