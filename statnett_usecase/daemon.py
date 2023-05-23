import time
from threading import Thread, Event
import atexit
from configuration.statnett_usecase_config import Configuration
from pydantic import validate_arguments
from statnett_usecase.energydataservice_api import get_energy_data
from statnett_usecase.datefunc import format_time,local_timestamp_now,get_period,local_timestamp_with_lag,date_diff_mins
import arrow
from typing import List
import matplotlib.pyplot as plt
import pandas as pd
import logging

logger =  logging.getLogger(__name__)

"""_______________________________________________________________
 daemon_process function. 
 _________________________________________________________________
 This function will run in the background to process the API 
 data every 5 mins until it receives an interruption signal.  
 _________________________________________________________________""" 
def daemon_process(config:Configuration,event:Event):
    logger.info('Daemon process started...')
    end_time = local_timestamp_with_lag(config)
    start_time,end_time = get_period(end_time,config)
    df = pd.DataFrame(columns = ['time','co2','renewables'])
    while True:
        if event.is_set():
            break       
        co2,renewables = get_energy_data(config,start_time,end_time)
        df.loc[len(df)] = {'time':time,'co2':co2,'renewables':renewables}
        plot_co2(df)
        time.sleep(config.interval*60)
        start_time=end_time
        start_time_arrow = arrow.get(start_time)
        end_time_arrow = date_diff_mins(start_time_arrow,config.interval)
        end_time = format_time(end_time_arrow)
    logger.info('Daemon process ended...')

""" plot_co2  To plot co2 against time """
def plot_co2(dataframe):
    dataframe.plot(x='time', y='co2',color ='red')
    plt.show()





    





