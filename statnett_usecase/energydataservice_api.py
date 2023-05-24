import requests
from configuration.statnett_usecase_config import Configuration
from pydantic import validate_arguments
import pandas as pd 
import json 
from typing import Dict,Tuple
import logging
import arrow
from statnett_usecase.datefunc import date_diff_mins,format_time

logger =  logging.getLogger(__name__)

"""_____________________________________________________________
  get_energy_data function.
________________________________________________________________
 This function connect with the url and fetch data for the given 
 time period (5 mins).  
________________________________________________________________""" 

@validate_arguments
def get_energy_data(config: Configuration,start: str, end: str) -> Tuple[float,float]:
    dynamic_url = f'{config.url}?start={start}&end={end}&limit=6'
    logger.info(f'Url is called: {dynamic_url}')
    response = requests.get(dynamic_url)
    result = response.json()
    co2,renewables = process_result(result)
    return co2,renewables

"""_____________________________________________________________
  get_energy_backfill_data function.
________________________________________________________________
 This function connect with the url and fetch data for long period.  
________________________________________________________________""" 

@validate_arguments
def get_energy_backfill_data(config: Configuration,start: str, end: str):
    dynamic_url = f'{config.url}?start={start}&end={end}'
    logger.info(f'Url is called: {dynamic_url}')    
    response = requests.get(dynamic_url)
    result = response.json()    
    result_df = split_data(result,start,end,config)
    return result_df


"""____________________________________________________________
 process_result function. 
_______________________________________________________________
 This function will receive the API output and aggregate CO2 
 and renewables.  
_______________________________________________________________"""
def process_result(result: Dict) ->Tuple[float,float]:
    total = result.get('total',0)
    limit = result.get('limit',0)
    dataset = result.get('dataset','')
    records = result.get('records', [])
    df = pd.json_normalize(records)
    agg = df.agg(
        sum_co2_emission = ('CO2Emission','average'),
        sum_prod_gt100 = ('ProductionGe100MW','sum'),
        sum_prod_lt100 = ('ProductionLt100MW','sum'),
        sum_solar = ('SolarPower','sum'),
        sum_offshore_wind = ('OffshoreWindPower','sum'),
        sum_onshore_wind = ('OnshoreWindPower','sum')      
     )#.reset_index()
    result_co2_emission = agg.at['sum_co2_emission','CO2Emission']
    result_renewables = agg.at['sum_solar','SolarPower'] +  agg.at['sum_offshore_wind','OffshoreWindPower'] + agg.at['sum_onshore_wind','OnshoreWindPower']
    return result_co2_emission,result_renewables    
   # co2_emission = agg[agg['index'].isin(['sum_co2_emission'])]['CO2Emission']

""" Split the result into 5 mins interval """
def split_data(result:json,start: str, end: str,config:Configuration ):
  df = pd.DataFrame(columns = ['time','co2','renewables'])  
  total = result.get('total',0)
  records = result.get('records', [])
  original_start_arrow = arrow.get(start)
  end_time_arrow = arrow.get(end)
  start_time_arrow = date_diff_mins(end_time_arrow,config.interval*-1)  
  while start_time_arrow >= original_start_arrow:
    end_time = format_time(end_time_arrow)
    start_time = format_time(start_time_arrow)
    print(start_time,end_time)
#    records = filter_data(records,start_time_arrow,end_time_arrow)
#    co2,renewables = process_result(records)
#    df.loc[len(df)] = {'time':end_time,'co2':co2,'renewables':renewables}
    end_time_arrow = start_time_arrow
    start_time_arrow = date_diff_mins(start_time_arrow,config.interval*-1)
  return df

 











                                           


    