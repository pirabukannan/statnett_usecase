import arrow
from arrow import Arrow
from configuration.statnett_usecase_config import Configuration
from typing import Tuple
from pydantic import validate_arguments

"""  return the current_time - delay mins (defined in configuration) in local tz """
@validate_arguments
def local_timestamp_with_lag(config: Configuration) -> Arrow:
    local_time = local_timestamp_now()
    local_time_delayed = date_diff_mins(local_time,config.delay*-1)
    return local_time_delayed

"""  return the current_time  in local tz """
def local_timestamp_now() -> Arrow:
    utc_time = arrow.utcnow()
    local_time = utc_time.to('Europe/Copenhagen')
    return local_time

"""  Calculate the new time based on the mins diff ( diff can be positive or negative) """
def date_diff_mins(current_time: Arrow, interval_mins:int ) -> Arrow:
    start_time = current_time.shift(minutes=interval_mins)
    return start_time


"""  Format the Arrow time object in a specific YYYY-MM-DDTHH:mm  format as return as string  """
def format_time(input_time: Arrow) -> str:
    return input_time.format('YYYY-MM-DDTHH:mm')


""" Return Arrow object from a string ( not useful now. might be usefull for backfill implementation) """
def local_time(input_datetime_str:str,tz: str) -> Arrow:    
    local_time = arrow.get(input_datetime_str, 'YYYY-MM-DDTHH:mm',tzinfo=tz)
    return local_time

"""  Return the period for a given arrow object and given interval.  """
def get_period(current_time: Arrow, interval: int) -> Tuple[str,str]:
    end_time = current_time
    start_time = date_diff_mins(end_time,interval * -1)
    end_time_str = format_time(end_time)
    start_time_str = format_time(start_time)
    return start_time_str,end_time_str