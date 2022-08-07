from enum import Enum

class PriceArea(Enum):
    DK1 = 'DK1'
    DK2 = 'DK2'
    DE = 'DE'
    
    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_ 