import os
import pandas as pd
import requests
from itertools import starmap
from etl.models import SpotPrice
from etl.enums import PriceArea
from datetime import timedelta
from etl.crud import CRUD
from sqlmodel import create_engine, SQLModel, Session

response = requests.get(
    url=f'https://api.energidataservice.dk/v2/dataset/Elspotprices?limit=100000000'
)

result = response.json()
records = result.get('records', [])

spotprices = list(SpotPrice(**r) for r in records)

engine = create_engine(os.getenv('DB_CONNECTION'))
SQLModel.metadata.create_all(engine)
crud_spotprice = CRUD(engine, SpotPrice)
# crud_spotprice.delete()
crud_spotprice.create(spotprices)

spotprices = crud_spotprice.read(filters=[SpotPrice.PriceArea == 'DK2'])
df = pd.DataFrame(p.dict() for p in spotprices)
print(df.HourDK.min(), df.HourDK.max())
print(df)