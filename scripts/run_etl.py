import os
import pandas as pd
import requests
from sqlmodel import create_engine, SQLModel
from etl.models import SpotPrice
from etl.crud import CRUD

def get_spotprices(url: str):
    response = requests.get(url=url)
    result = response.json()
    records = result.get('records', [])
    rows = list(SpotPrice(**r) for r in records)
    return rows

if __name__ == '__main__':
    test = True

    if not test:
        rows = get_spotprices(
            url='https://api.energidataservice.dk/v2/dataset/Elspotprices?limit=100000000'
        )
        engine = create_engine(os.getenv('DB_CONNECTION'))
        SQLModel.metadata.create_all(engine)
        crud = CRUD(engine=engine, table=SpotPrice)
        crud.create(rows)
        
    else:
        data = get_spotprices('https://api.energidataservice.dk/v2/dataset/Elspotprices')
        data = pd.DataFrame(r.dict() for r in data)
        print(data)