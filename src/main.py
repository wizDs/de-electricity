import os
import pandas as pd
import requests
from etl.models import SpotPrice
from etl.crud import CRUD
from sqlmodel import create_engine, SQLModel

response = requests.get(
    url=f'https://api.energidataservice.dk/v2/dataset/Elspotprices?limit=100000000'
)

result = response.json()
records = result.get('records', [])
rows = list(SpotPrice(**r) for r in records)

engine = create_engine(os.getenv('DB_CONNECTION'))
SQLModel.metadata.create_all(engine)
crud = CRUD(engine=engineengine, table=SpotPrice)
crud.create(rows)
