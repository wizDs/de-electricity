import os
import pandas as pd
import requests
from sqlmodel import create_engine, SQLModel
from etl.models import SpotPrice
from etl.crud import CRUD
from etl import extract

if __name__ == '__main__':
    test = False
    db_connection = os.getenv('DB_CONNECTION')
    endpoint = 'https://api.energidataservice.dk/v2/dataset/Elspotprices'
    limit = 100_000_000

    if not test:
        rows = extract(
            url=f'{endpoint}?limit={limit}',
            model=SpotPrice
        )
        engine = create_engine(db_connection)
        SQLModel.metadata.create_all(engine)
        crud = CRUD(engine=engine, table=SpotPrice)
        crud.create(rows=rows)
        
    else:
        data = extract(url=endpoint, model=SpotPrice)
        data = pd.DataFrame(r.dict() for r in data)
        print(data)