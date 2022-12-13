from enum import Enum

class PipelineType(Enum):
    
    SPOTPRICE = "spotprice"
    POWERSYSTEM = "powersystem"
    WEATHER = "weather"