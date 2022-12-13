import requests
from datetime import date
from sqlmodel import SQLModel
import schemas

class ElectricityClient:
    def __init__(self) -> None:
        self.baseurl = f'https://api.energidataservice.dk/dataset'

    @staticmethod
    def get_records(url: str, params: dict = None) -> list:
        response = requests.get(url=url, params=params)
        result = response.json()
        records = result.get('records', [])
        return records

    @staticmethod
    def records_to_sqlmodel(records: list, model: SQLModel) -> list[SQLModel]:
        return list((model(**r) for r in records))


class SpotpriceClient(ElectricityClient):

    def __init__(self) -> None:
        super().__init__()
        self.endpoint = f'{self.baseurl}/Elspotprices'
        self.limit = 100_000_000
        self.schema = schemas.SpotPrice
    
    def get(self, start_date: date, end_date: date) -> list[schemas.SpotPrice]:
        params = dict(
            start=start_date,
            end=end_date,
            limit=self.limit
        )
        records = self.get_records(self.endpoint, params=params)
        return self.records_to_sqlmodel(records, self.schema)


class PowerSystemClient(ElectricityClient):
        
    def __init__(self) -> None:
        super().__init__()
        self.endpoint = f'{self.baseurl}/PowerSystemRightNow'
        self.limit = 500_000
        self.schema = schemas.PowerSystem

    def get(self, start_date: date, end_date: date) -> list[schemas.PowerSystem]:
        params = dict(
            start=f'{start_date}T00:00',
            end=f'{end_date}T00:00',
            limit=self.limit
        )
        records = self.get_records(self.endpoint, params=params)
        return self.records_to_sqlmodel(records, self.schema)
