import pandas as pd
import pytest 

from pipelines import Config, SpotPricePipeline
from loaders import SpotpriceClient
from enums import PipelineType

config = Config(    
    year='2022',
    month='10',
    path='./data',
    datatype=PipelineType.SPOTPRICE
)

def test_electricity_api():
    data = SpotpriceClient().get(config.start_date(), config.end_date())
    assert len(data) > 0
    
def test_duplicated_data():
    data = SpotpriceClient().get(config.start_date(), config.end_date())
    data = pd.DataFrame(row.dict() for row in data)
    assert not data.duplicated().any()