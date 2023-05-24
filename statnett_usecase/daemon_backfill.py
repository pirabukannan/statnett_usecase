import time
from threading import Thread, Event
import atexit
from configuration.statnett_usecase_config import Configuration
from pydantic import validate_arguments
from statnett_usecase.energydataservice_api import get_energy_backfill_data
from statnett_usecase.datefunc import local_time,format_time,local_timestamp_now,get_period,local_timestamp_with_lag,date_diff_mins
import arrow
from typing import List
import pandas as pd
import logging

logger =  logging.getLogger(__name__)

"""_______________________________________________________________
 daemon_backfill_process function. 
 _________________________________________________________________
 This function will run in the background to process the API 
 data for the time from now till back fill time defined in config 
 or  until it receives an interruption signal.  
 _________________________________________________________________""" 
def daemon_backfill_process(config:Configuration,event:Event):
    logger.info('Daemon backfill process started...')
    end_time = local_timestamp_with_lag(config)
    original_start = local_time(config.backfill_date,tz=config.tz)
    start_time,end_time = get_period(end_time,config.backfill_interval)
    start_time_arrow = arrow.get(start_time)
    while True:
        if event.is_set():
            break 
        if start_time_arrow <= original_start:
            break        
        # find a way to compare original backfill with start and take the greater one
        df = get_energy_backfill_data(config,start_time,end_time)    
        time.sleep(60)
        end_time=start_time
        end_time_arrow = arrow.get(end_time)
        start_time_arrow = date_diff_mins(end_time_arrow,config.backfill_interval*-1)
        if start_time_arrow < original_start:
            start_time_arrow = original_start
        start_time = format_time(start_time_arrow)
    logger.info('Daemon process ended...')
