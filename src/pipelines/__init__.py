import logging
from typing import Optional
import pandas as pd
from datetime import date,  timedelta
from dateutil.relativedelta import relativedelta
import loaders.electricity as electricity
import pathlib
import uuid
from pydantic import BaseModel

class Config(BaseModel):
    year: int
    month: Optional[int]
    path: str
    datatype: str
    
    def start_date(self):
        return date(self.year, self.month, 1)
    
    def end_date(self):
        return self.start_date() + relativedelta(months=+1) - timedelta(days=1)

    def get_full_path(self) -> pathlib.Path:
        full_path = f"{self.path}/{self.datatype}/year={self.year}/month={self.month}/"
        return pathlib.Path(full_path)

class DataPipeline:

    def __init__(self, config: Config) -> None:
        self.config = config
   
    def persist_data(self, data: pd.DataFrame) -> None:
        path = self.config.get_full_path()
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        data.to_parquet(path=path / f"{uuid.uuid4()}.parquet")

    def run(self):
        raise NotImplementedError()


class SpotPricePipeline(DataPipeline):

    def __init__(self, config: Config) -> None:
        super().__init__(config)
    
    def run(self):
        start_date = self.config.start_date()
        end_date = self.config.end_date()
        
        # Extract data from energidataservice
        logging.info(f"getting {self.config.datatype}-data for the period {start_date}-{end_date}")
        data = electricity.SpotpriceClient().get(start_date, end_date)
        data = pd.DataFrame(row.dict() for row in data)

        # persist data
        self.persist_data(data=data)


class PowerSystemPipeline(DataPipeline):
    
    def __init__(self, config: Config) -> None:
        super().__init__(config)
    
    def run(self):
        start_date = self.config.start_date()
        end_date = self.config.end_date()
        
        # Extract data from energidataservice
        logging.info(f"getting {self.config.datatype}-data for the period {start_date}-{end_date}")
        data = electricity.PowerSystemClient().get(start_date, end_date)
        data = pd.DataFrame(row.dict() for row in data)

        # persist data
        self.persist_data(data=data)

        