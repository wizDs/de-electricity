from datetime import date, datetime, timedelta
import enum
class PeriodFrequency(enum.Enum):
    QUARTER = '3M'
    MONTH = 'M'
    DAY = 'D'
    HOUR = 'H'

def get_month_interval(period: date|datetime) -> tuple[date, date]:
    # TODO: Make more generic
    next_period = period + timedelta(days=1)
    start_date = date(period.year, period.month, 1)
    end_date = date(next_period.year, next_period.month, 1)
    return start_date, end_date

def get_day_interval(period: date|datetime) -> tuple[date, date]:
    return (period, period+timedelta(day=1))

def get_interval(freq: PeriodFrequency, d: date) -> tuple[date, date]:
    match freq: 
        case PeriodFrequency.MONTH: 
            return get_month_interval(d)
        case PeriodFrequency.DAY: 
            return get_day_interval(d)
        case _: 
            raise NotImplementedError(f'{freq.value} not implemented yet')

