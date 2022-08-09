import os
import pandas as pd
import requests
from datetime import date, timedelta
from sqlmodel import create_engine, SQLModel
from etl.models import PowerSystemRightNow
from etl.crud import CRUD
from etl import extract

if __name__ == '__main__':
    test = False
    db_connection = os.getenv('DB_CONNECTION')
    endpoint = 'https://api.energidataservice.dk/v2/dataset/PowerSystemRightNow'
    limit = 100_000
    periods = pd.date_range(start='2018-01-01', end='2022-07-01', freq='M')

    for period in periods:
        next_period = period + timedelta(days=1)
        start_date = date(period.year, period.month, 1)
        end_date = date(next_period.year, next_period.month, 1)
        print(start_date, end_date)

        if not test:
            rows = extract(
                url=f'{endpoint}?start={start_date}&end={end_date}&limit={limit}',
                model=PowerSystemRightNow
            )
            engine = create_engine(db_connection)
            SQLModel.metadata.create_all(engine)
            crud = CRUD(engine=engine, table=PowerSystemRightNow)
            crud.create(rows=rows)
            
        else:
            data = extract(
                url=f'{endpoint}?start={start_date}&end={end_date}', 
                model=PowerSystemRightNow
            )
            data = pd.DataFrame(r.dict() for r in data)
            print(data)