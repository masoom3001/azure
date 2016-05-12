import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from selenium import webdriver
import pytz
import win32api


class NordPool:
    
    DATE_FORMAT = "%d-%m-%Y"
    LAST_TIME_STAMP = 1462665600
    url = "http://www.nordpoolspot.com/Market-data1/Elspot/Area-Prices/FI/Hourly/?view=table"
    
    def __init__(self):
        self.browser = webdriver.Firefox()
        self.browser.get(NordPool.url)
    

    def getEnergyPriceForecast(self):
        self.browser.get(NordPool.url)
        win32api.Sleep(10000, True)
        table = self.browser.find_element_by_xpath("//table[@id='datatable']")
        #win32api.Sleep(20000, True)
        #headers = table.find_elements_by_xpath(".//thead/tr/th[@class='']")
        headers = table.find_elements_by_xpath(".//thead/tr/th")
        headers = headers[1:len(headers)]
        #for date in headers:
        #    print (date.text)
        dates = []
        # Nord pool uses CET/CEST time zone for storing data which is same as in Oslo
        dataTimeZone = pytz.timezone('Europe/Oslo')
        # converting time from CET to UTC
        # Since IoTTicket use UTC time as timestamp
        for date in headers:
            dateTime = datetime.strptime(date.text, NordPool.DATE_FORMAT)
            dataLocalTime = dataTimeZone.localize(dateTime)
            dateInUTC = dataLocalTime.astimezone(pytz.utc)
            dates.append(int(dateInUTC.timestamp()))

        rows = table.find_elements_by_xpath(".//tbody/tr")
        valueRows = rows[0:24]
        powerPriceList = []
        for tr in valueRows:
            tdList = tr.find_elements_by_xpath(".//td")
            tdList = tdList[1:len(tdList)]
            for td in tdList:
                powerPriceList.append(float(td.text.replace(',', '.')))
        
        timeStampInSecond = []
        for hour in range(24):
            for date in dates:
                timeStampInSecond.append( date + hour*3600)
        
        NordPool.LAST_TIME_STAMP = dates[0]
        df = pd.DataFrame({'timestamp' : timeStampInSecond, 'price' : powerPriceList})
        return df


    def isNewDataAvailable(self):
        self.browser.get(NordPool.url)
        win32api.Sleep(10000, True)
        table = self.browser.find_element_by_xpath("//table[@id='datatable']")
        headers = table.find_elements_by_xpath(".//thead/tr/th")
        headers = headers[1:len(headers)]
        #headers = table.find_elements_by_xpath(".//thead/tr/th[@class='']")
        dates = []
        # Nord pool uses CET/CEST time zone for storing data which is same as in Oslo
        dataTimeZone = pytz.timezone('Europe/Oslo')
        # converting time from CET to UTC
        # Since IoTTicket use UTC time as timestamp
        for date in headers:
            dateTime = datetime.strptime(date.text, NordPool.DATE_FORMAT)
            dataLocalTime = dataTimeZone.localize(dateTime)
            dateInUTC = dataLocalTime.astimezone(pytz.utc)
            dates.append(int(dateInUTC.timestamp()))

        if dates[0] > NordPool.LAST_TIME_STAMP :
            return True
        else: 
            return False

