import os
import pandas as pd
from datetime import date
from sqlmodel import create_engine, SQLModel
from etl.crud import CRUDSpotPrice, CRUDPowerSystem
from etl.date_interval import PeriodFrequency, get_interval
from etl.client.electricity import ElectricityClient


if __name__ == '__main__':
    dryrun = False
    start = '2020-05-01'
    end = '2022-09-01'
    freq = PeriodFrequency.MONTH
    periods = pd.date_range(start=start, end=end, freq=freq.value)
    now = date.today()

    # http client
    client = ElectricityClient()

    # sql crud
    db_connection = os.getenv('DB_CONNECTION')
    engine = create_engine(db_connection)
    SQLModel.metadata.create_all(engine)
    sql_crud_spotprice = CRUDSpotPrice(engine=engine)
    sql_crud_powersystem = CRUDPowerSystem(engine=engine)

    for period in periods:
        # Determine period
        start_date, end_date = get_interval(freq, period)
        print(start_date, end_date)

        # Extract data from energidataservice
        powersystem = client.extract_powersystem(start_date, end_date)
        spotprices = client.extract_spotprices(start_date, end_date)

        # Transform (None yet)
        
        # Load to our database
        if not dryrun:
            sql_crud_powersystem.create(rows=powersystem)
            sql_crud_spotprice.create(rows=spotprices)