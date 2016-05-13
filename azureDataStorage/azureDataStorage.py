# -*- coding: utf-8 -*-

import nordPool
import threading, time
import pandas as pd
from datetime import datetime
import win32service
import win32serviceutil
import win32api
import win32event
import traceback
from azure.storage.table import TableService
from azure.storage.table.models import Entity, EntityProperty, EdmType

next_call = time.time()
webScraper = nordPool.NordPool()
POWER_PRICE_FORECAST_DEVICE_NAME = "Forecast on price of power"
PREDICTION_DATA = "Prediction data"
TIME_DURATION_FOR_SLEEP = 3600
ACCOUNT_NAME = "####"
ACCOUNT_KEY = "####"
TABLE_NAME = "priceofpower"
PARTITION_KEY = "predictedPrice"
table_service = TableService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)


class forcastPricePowerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "azureDataStore"
    _svc_display_name_ = "Store forecast data on price of power in Azure cloud"
    _svc_description_ = "Store forecast data on price of power per mega watt hour from www.nordpoolspot.com webpage in Azure cloud"
    def __init__(self, args):
           win32serviceutil.ServiceFramework.__init__(self, args)
           self.logInfo('Initialization done')
           self.stop_event = win32event.CreateEvent(None, 0, 0, None)           

    def SvcStop(self):
         self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
         self.logInfo('stopping')
         self.stop()
         self.logInfo('stopped')
         win32event.SetEvent(self.stop_event)
         self.ReportServiceStatus(win32service.SERVICE_STOPPED)
           
    def logInfo(self, msg):
        import servicemanager
        servicemanager.LogInfoMsg(str(msg))     

    def logError(self, msg):
        import servicemanager
        servicemanager.LogErrorMsg(str(msg))
        
    def performPeriodicTask(self):
        global table_service,webScraper     
        
        forecastPricePowerDf = pd.DataFrame()       
               
        if webScraper.isNewDataAvailable():
            forecastPricePowerDf = webScraper.getEnergyPriceForecast()
            if not forecastPricePowerDf.empty:
                storeDataToAzure(forecastPricePowerDf)
    
        

    def sleep(self, sec):      
        win32api.Sleep(int(sec*1000), True)
             
    def SvcDoRun(self):   
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        try:
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            self.logInfo('start')
            self.start()
            self.logInfo('wait')
            win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)
            self.logInfo('done')
        except Exception as e:
            self.logInfo('Exception : %s' % e)
            self.logInfo(''.join(traceback.format_stack()))
            self.SvcStop()

    def start(self):
        self.runflag=True
        while self.runflag:
            try:
                startTime = time.time()
                self.performPeriodicTask()
                currentTime = time.time()
                self.sleep(startTime + TIME_DURATION_FOR_SLEEP - currentTime)
                self.logInfo("I'm alive ...\n Current time: %s"  % (datetime.now()))
            except requests.exceptions.HTTPError as e: 
                self.logError('HTTPError occured: %s' % e)
                self.logInfo(''.join(traceback.format_stack()))
                # sleep for 1 hours
                currentTime = time.time()
                self.sleep(startTime + TIME_DURATION_FOR_SLEEP - currentTime)
                self.logInfo("I'm alive ...\n Current time: %s"  % (datetime.now()))

    def stop(self): 
         self.runflag=False
         self.logInfo("I'm done")

      
def ctrlHandler(ctrlType):
    return True
                  


def storeDataToAzure(dataFrame):
    global table_service
    prices = dataFrame['price']
    timeStamp = dataFrame['timestamp']
    for idx in prices.index:
        entity = Entity()
        entity.PartitionKey = PARTITION_KEY
        entity.RowKey = str(idx + 1)
        #entity.value = prices[idx]
        entity.value = EntityProperty(EdmType.DOUBLE, prices[idx])
        entity.ts = EntityProperty(EdmType.DATETIME, datetime.utcfromtimestamp(timeStamp[idx]))
        table_service.insert_or_replace_entity(TABLE_NAME, entity)

def initialStep():
    global webScraper, table_service 
    priceForecastDf = webScraper.getEnergyPriceForecast()
    table_service.create_table(TABLE_NAME)
    storeDataToAzure(priceForecastDf)
            

if __name__ == '__main__':   

    initialStep()
    win32api.SetConsoleCtrlHandler(ctrlHandler, True)  
    win32serviceutil.HandleCommandLine(forcastPricePowerService)

