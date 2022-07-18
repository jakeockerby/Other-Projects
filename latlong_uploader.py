# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 08:26:29 2022

@author: Jake
"""

import pandas as pd
import numpy as np
import configparser
import pyodbc
import requests
from time import sleep
import json
from datetime import datetime, timedelta
from datetime import date
from geopy.geocoders import Nominatim
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import dtale


# Accessing the SQL Server
# Initialize the ConfigParser
config = configparser.ConfigParser()
# Read the config
#config.read('/home/portaladmin/scripts/python/Data_Science/config')
config.read(r'Z:/Technology/Data Science/Config/config.txt')
username = config.get('DEFAULT', 'username')
password = config.get('DEFAULT', 'password')
server = config.get('DEFAULT', 'server')
database = config.get('DEFAULT', 'database')
# Create connections to the SQL db
# Adjust the parameters to connect to different SQL dbs
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};'
connection_string += 'SERVER=' + server + ';'
connection_string += 'DATABASE=' + database + ';'
connection_string += 'UID=' + username + ';'
connection_string += 'PWD=' + password + ';'
cnxn = pyodbc.connect(connection_string)
cursor = cnxn.cursor()





# Function that inserts dataframe into the database
def insert_SQL(df):
    for row in df.itertuples():
        product = row[1]
        survey_loc = row[2]
        latitude = row[3]
        longitude = row[4]

        sql = """INSERT INTO dat.t_lat_long (product, survey_location,
        latitude, longitude)
        VALUES (?,?,?,?)"""
        val = (product, survey_loc, latitude, longitude)
        cursor.execute(sql, val)
        cursor.commit()
    print("upload_complete")


    

# Grabbing regions within new countries
portal_query = """ 
SELECT [product]
      ,[survey_location]
  FROM [dat].[t_locations]
  WHERE product in ('Kids South Africa', 'Parents South Africa')
"""    
    
portal_Query = pd.read_sql(portal_query, cnxn)
portal_locations = pd.DataFrame(portal_Query)  

# Replacing mendoza city as it was causing issues
portal_locations['survey_location'] = portal_locations['survey_location'].replace('Mendoza city', 'Mendoza')
print(len(portal_locations))

# Loading geolocator
geolocator = Nominatim(user_agent="myGeocoder")


# Adding country to the region names, so when searched the results are more precise
new_locations = []
for row in portal_locations.itertuples():
    if 'Kids' in row[1]:
        country = row[1].split('Kids ')[1]
    else:
        country = row[1].split('Parents ')[1]
        
        
    new = row[2] + ', ' + country
    new_locations.append(new)



# Getting latitude and longitude for each region and adding them to lists
lats = []
longs = []
c = 1
for region in new_locations:
    print(c)
    print(region)
    if region == 'North-West Province, South Africa':
        region = 'Vryburg, South Africa'
    location = geolocator.geocode(region)
    lat = location.latitude
    long = location.longitude
    lats.append(lat)
    longs.append(long)
    c += 1
    
portal_locations['survey_location'] = new_locations

# Putting mendoza city back to where it was
portal_locations['survey_location'] = portal_locations['survey_location'].replace('Mendoza', 'Mendoza city')

# Create new columns
portal_locations['latitude'] = lats
portal_locations['longitude'] = longs




# Insert dataframe into database
insert_SQL(portal_locations)