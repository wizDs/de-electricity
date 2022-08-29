import requests
from datetime import date
from sqlmodel import SQLModel
import etl.models

class ElectricityClient:
    def __init__(self, version: str='v2') -> None:
        self.version = version
        self.baseurl = f'https://api.energidataservice.dk/{version}/dataset'

    def extract_spotprices(self, start_date: date, end_date: date) -> list[etl.models.SpotPrice]:
        endpoint = f'{self.baseurl}/Elspotprices'
        limit = 100_000_000
        return self.__extract(
            url=f'{endpoint}?start={start_date}&end={end_date}&limit={limit}',
            model=etl.models.SpotPrice
        )

    def extract_powersystem(self, start_date: date, end_date: date) -> list[etl.models.PowerSystem]:
        endpoint = f'{self.baseurl}/PowerSystemRightNow'
        limit = 500_000
        return self.__extract(
            url=f'{endpoint}?start={start_date}T00:00&end={end_date}T00:00&limit={limit}',
            model=etl.models.PowerSystem
        )

    @staticmethod
    def __extract(url: str, model: SQLModel) -> list[SQLModel]:
        response = requests.get(url=url)
        result = response.json()
        records = result.get('records', [])
        rows = (model(**r) for r in records)
        return list(rows)

    