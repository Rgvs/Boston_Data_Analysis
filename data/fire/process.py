import csv
import pandas as pd
import numpy as np
import glob, os
from uszipcode import ZipcodeSearchEngine
from datetime import date
from geopy.geocoders import Nominatim
import calendar
import time
geolocator = Nominatim()
location = geolocator.geocode("161 CAMBRIDGE Boston 2114")
print(location.latitude)
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']

def getLatLong(x):
    #print(x)
    #location = geolocator.geocode(x)
    try:
        return geolocator.geocode(x).latitude,geolocator.geocode(x).longitude
    except:
        return "",""
def weekday(x):
    year=0
    month=0
    day=0
    a = x.split("/")
    b = x.split("-")
    #c = x.to_string().split("")
    if(len(a)>1):
        month = int(a[0])
        
        day= int(a[1])
        
        year = 2000+int(a[2])
    elif(len(b)>1):
        month = int(b[0])
        day= int(b[1])
        year = 2000+int(b[2])
        
    offset = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    week   = ['Sunday', 
              'Monday', 
              'Tuesday', 
              'Wednesday', 
              'Thursday',  
              'Friday', 
              'Saturday']
    afterFeb = 1
    if month > 2: afterFeb = 0
    aux = year - 1700 - afterFeb
    # dayOfWeek for 1700/1/1 = 5, Friday
    dayOfWeek  = 5
    # partial sum of days betweem current date and 1700/1/1
    dayOfWeek += (aux + afterFeb) * 365                  
    # leap year correction    
    dayOfWeek += aux / 4 - aux / 100 + (aux + 100) / 400     
    # sum monthly and day offsets
    dayOfWeek += offset[month - 1] + (day - 1)               
    dayOfWeek %= 7
    return week[int(dayOfWeek)],months[month-1]

def readMyFile(filename):
    with open(filename) as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        next(csvReader)
        index=0
        myFile = open('output.csv', 'w',newline='')
        data =['Incident Number','Day','Month','Hour','Lat','Long']
        with myFile: 
            writer = csv.writer(myFile)
            writer.writerow(data)
            for row in csvReader:
                index = index+1
                if(index<15667):
                    continue
                time.sleep(0.99)
                r = []
                r.append(str(row[0]))
                r.append(str(row[2]).split(":")[0])
                d,m = weekday(str(row[1]))
                r.append(d)
                r.append(m)
                lat,long= getLatLong(str(row[5]).strip()+" "+str(row[6]).strip()+" "+str(row[3]).strip()+" "+str(row[4]).strip())
                r.append(lat)
                r.append(long)
                
                if(index%1000==0):
                    print(index)
                
                writer.writerow(r)

#data =[['Incident Number','Day','Month','Hour','Lat','Long']] 
readMyFile('2016-bostonfireincidentopendata-1.csv')
