from statnett_usecase.daemon import daemon_process
from threading import Thread, Event
import atexit
import time
import os
from configuration.statnett_usecase_config import Configuration
import yaml 
import signal
import sys
import logging 


stop_event = Event() # Event to indicate the terminate signal and end the program gracefully.
logging.basicConfig(filename='statnett_usecase.log',format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
logger =  logging.getLogger(__name__)

"""_______________________________________________________________
 Main function. 
 _________________________________________________________________
 This function will initiate the daemon thread to process the 
 energy data. 
 If an interrupt signal is received the stop event is set 
 and the stop_background process is called before the main program
 is completed. 
__________________________________________________________________"""
def main():
    # run the main thread for a while

    logger.info('Main thread started...')    
    signal.signal(signal.SIGTERM, signal_term_handler)
    signal.signal(signal.SIGINT, signal_term_handler)
    config = config_load()    
    thread = Thread(target=daemon_process(config,stop_event), daemon=True, name="Background")
    thread.start()  
    atexit.register(stop_background, stop_event, thread)
    logger.info('Main thread completed...') 
    print('Main thread completed')


""" config_path function will return the current working directory """
def config_path() -> str:
    cwd = os.getcwd()
    return cwd


""" config_load will load the configuration file into a pydantic class"""
def config_load() -> Configuration:
    with open("statnett_config.yaml","r") as f:
        data = yaml.safe_load(f)
        config_data = data["configuration"]
    configuration = Configuration(**config_data)
    return configuration   


""" signal_term_handler function will receive the interruption signal and set the  stop event.  """
def signal_term_handler(signal, frame):
    logger.info('SIGTERM signal received and program exit process started.. ')
    stop_event.set()
    #sys.exit(0)

""" stop_background function is called when the main thread is terminated and help to cleanup. """ 
def stop_background(stop_event, thread):
    thread.join()
    logger.info('Daemon thread stopped gracefully')




if __name__ == "__main__":
    main()
