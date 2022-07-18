# -*- coding: utf-8 -*-
"""
Created on Fri Sep 10 09:14:01 2021

@author: Jake
"""
import pandas as pd
import numpy as np
import configparser
import pyodbc
import requests
from time import sleep
import json
from datetime import datetime
from datetime import date
from geopy.geocoders import Nominatim
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


pd.set_option('display.max_rows', 635)
pd.set_option('display.max_columns', 20)


# Function that inserts dataframe into the database
def insert_SQL(df):
    for row in df.itertuples():
        country_code_ = row[1]
        country_name_ = row[2]
        zone_name_ = row[3]
        gmt_offset_ = row[4]
        dst_ = row[5]
        dst_start_ = row[6]
        dst_end_ = row[7]
        address_ = row[8]
        s_location_ = row[9]
        p_location_ = row[10]
        date_uploaded_ = row[11]

        sql = """INSERT INTO dat.t_timezone_data (countryCode, countryName,
        zoneName, gmtOffset, dst_in_use, dst_start, dst_end, address,
        survey_location, portal_location, date_uploaded)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)"""
        val = (country_code_, country_name_, zone_name_, gmt_offset_, dst_,
               dst_start_, dst_end_, address_, s_location_, p_location_,
               date_uploaded_)
        cursor.execute(sql, val)
        cursor.commit()
    print("upload_complete")



# Function to return the API key
def get_api_key(key_name, config_section='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(r'Z:/Technology/Data Science/Config/API_config.txt')
    key = config.get(config_section, key_name)
    return key




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

# Getting locations we need to match to from database
portal_query = """ 
SELECT [product]
      ,[product_id]
      ,[survey_location]
      ,[portal_location]
  FROM [dat].[t_locations]

"""
portal_Query = pd.read_sql(portal_query, cnxn)
portal_locations = pd.DataFrame(portal_Query)

# Copying the original dataframe as we will need this later
portal_locs = portal_locations.copy()

# Remove some dodgy text that helps with the fuzzy matching
portal_locs['survey_location'] = portal_locs['survey_location'].apply(lambda x: x.split(' (')[0])
portal_locs['survey_location'] = portal_locs['survey_location'].apply(lambda x: x.split(' do Brasil')[0])
portal_locs['survey_location'] = portal_locs['survey_location'].replace('Islas Canarias', 'Canarias')

# Key for TimezoneDB API
key = get_api_key('timezone_api_key')


# Get information for all the countries we need
part1 = requests.get('https://api.timezonedb.com/v2.1/list-time-zone?key={}&format=json'.format(key))
data = json.loads(part1.text)
df = pd.DataFrame(data['zones'])
df = df.drop('timestamp', axis=1)

# Divide by 3600 to get offset in hours
df['gmtOffset'] = df['gmtOffset']/3600


regions = ['Australia',
  'Brazil',
  'Canada',
  'China',
  'Germany',
  'Spain',
  'France',
  'United Kingdom',
  'India',
  'Italy',
  'Indonesia',
  'Japan',
  'South Korea',
  'Mexico',
  'Philippines',
  'Poland',
  'Russia',
  'United States']

# Locate only the countries on the portal
df = df.loc[df['countryName'].isin(regions)]


# Create lists to store daylight saving time information
dst_used = []
dst_start = []
dst_end = []

# for every row in the dataframe, get dst info for the zone
for row in df.itertuples():
    print(row.zoneName)
    part2 = requests.get('https://api.timezonedb.com/v2.1/get-time-zone?key={0}&format=json&by=zone&zone={1}'.format(key, row.zoneName))
    dst_data = json.loads(part2.text)
    print(dst_data['dst'])
    
    # Append whether dst is in use to the database
    dst_used.append(dst_data['dst'])
    
    # if zoneEnd is not null (no dst)
    if dst_data['zoneEnd'] != None:
        date1 = datetime.utcfromtimestamp(dst_data['zoneStart']).strftime('%Y-%m-%d %H:%M:%S')
        date2 = datetime.utcfromtimestamp(dst_data['zoneEnd']).strftime('%Y-%m-%d %H:%M:%S')
        print(date2)
        dst_start.append(date1 + ' UTC+0')
        dst_end.append(date2 + ' UTC+0')
        sleep(1)
    else:
        dst_start.append('N/A')
        dst_end.append('N/A')
        sleep(1)
     

df['dst?'] = dst_used
df['dst?'] = df['dst?'].replace({'1': 'Yes', '0': 'No'})


df['dst_start'] = dst_start
df['dst_end'] = dst_end

df['zoneName'] = df['zoneName'].apply(lambda x: x.split('/')[-1])
df['zoneName'] = df['zoneName'].str.replace('_', ' ')
df['zoneName'] = df['zoneName'].str.replace('Canary', 'Santa Cruz de Tenerife')

locs = []
for country, zone in zip(list(df['countryName']),list(df['zoneName'])):
    geolocator = Nominatim(user_agent="myGeocoder")
    if country == 'Brazil':
        location = geolocator.geocode('{0}, {1}'.format(zone, country), addressdetails=True, language='pt')
    elif country == 'Spain':
        location = geolocator.geocode('{0}, {1}'.format(zone, country), addressdetails=True, language='es')
    else:
        location = geolocator.geocode('{0}, {1}'.format(zone, country), addressdetails=True, language='en')
    
    address = []
    for i in location.raw['address']:
        if i != 'country':
            address.append(location.raw['address'][i])
    address  = ','.join(address)
        
    locs.append(address)

df['address'] = locs
print(df)


survey_ls = []
portal_ls = []
# conf = []
for line in list(df['address']):
    scores = []
    matches = []
    for word in line.split(','):
        match = process.extractOne(word, list(portal_locs['survey_location'].unique()), scorer=fuzz.token_sort_ratio)[0]
        score = process.extractOne(word, list(portal_locs['survey_location'].unique()), scorer=fuzz.token_sort_ratio)[1]
        
        matches.append(match)
        scores.append(score)
        
    if max(scores) >= 85:
        idx = scores.index(max(scores))
        new_idx = list(portal_locs['survey_location']).index(matches[idx])
        survey_ls.append(list(portal_locations['survey_location'])[new_idx])
        portal_ls.append(list(portal_locations['portal_location'])[new_idx])
        # conf.append(max(scores))
    else:
        survey_ls.append('N/A')
        portal_ls.append('N/A')
        
df['survey_location'] = survey_ls
df['portal_location'] = portal_ls
df['date_uploaded'] = date.today().strftime('%Y-%m-%d')
# df['confidence'] = conf

print(df)

insert_SQL(df)