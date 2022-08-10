import os
import pandas as pd
import requests
from datetime import date, datetime, timedelta
from sqlmodel import create_engine, SQLModel
from etl.models import SpotPrice, PowerSystem
from etl.crud import CRUD
from etl import extract

def extract_spotprices(start_date: date, end_date: date) -> list[SpotPrice]:
    endpoint = 'https://api.energidataservice.dk/v2/dataset/Elspotprices'
    limit = 100_000_000
    return extract(
        url=f'{endpoint}?start={start_date}&end={end_date}&limit={limit}',
        model=SpotPrice
    )

def extract_powersystem(start_date: date, end_date: date) -> list[PowerSystem]:
    endpoint = 'https://api.energidataservice.dk/v2/dataset/PowerSystemRightNow'
    limit = 500_000
    return extract(
        url=f'{endpoint}?start={start_date}T00:00&end={end_date}T00:00&limit={limit}',
        model=PowerSystem
    )

def get_month_interval(period: date|datetime) -> tuple[date, date]:
    # TODO: Make more generic
    next_period = period + timedelta(days=1)
    start_date = date(period.year, period.month, 1)
    end_date = date(next_period.year, next_period.month, 1)
    return start_date, end_date

def get_day_interval(period: date|datetime) -> tuple[date, date]:
    return (period, period+timedelta(day=1))

if __name__ == '__main__':
    import enum
    class PeriodFrequency(enum.Enum):
        QUARTER = '3M'
        MONTH = 'M'
        DAY = 'D'
        HOUR = 'H'

    dryrun = False
    start = '2018-10-01'
    end = '2018-12-01'#'2022-08-01'
    freq = PeriodFrequency.MONTH
    periods = pd.date_range(start=start, end=end, freq=freq.value)

    # sql crud
    db_connection = os.getenv('DB_CONNECTION')
    engine = create_engine(db_connection)
    SQLModel.metadata.create_all(engine)
    sql_crud_spotprice = CRUD(engine=engine, table=SpotPrice)
    sql_crud_powersystem = CRUD(engine=engine, table=PowerSystem)

    for period in periods:
        # Determine period
        match freq: 
            case PeriodFrequency.MONTH: start_date, end_date = get_month_interval(period)
            case PeriodFrequency.DAY: start_date, end_date = get_day_interval(period)
            case _: raise NotImplementedError(f'{freq.value} not implemented yet')

        print(start_date, end_date)

        # Extract data from energidataservice
        powersystem = extract_powersystem(start_date, end_date)
        spotprices = extract_spotprices(start_date, end_date)

        # Transform (None yet)

        # Load to our database
        if not dryrun:
            sql_crud_powersystem.create(rows=powersystem)
            sql_crud_spotprice.create(rows=spotprices)