# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 13:16:52 2021

@author: Jake
"""

import pandas as pd
import numpy as np
import configparser
import pyodbc
import requests
import json
from time import sleep
from datetime import date

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



def insert_SQL(df):
    for row in df.itertuples():
        data_source_ = row.data_source
        search_name_ = row.search_name
        search_subcategory_ = row.search_subcategory
        question_reference_ = row.question_reference
        question_results_ = row.question_results
        date_collected_ = row.date_collected
        current_data_ = row.current_data

        sql = """INSERT INTO dat.t_api_data (data_source,
        search_name, search_subcategory, question_reference,
        question_results, date_collected, current_data)
        VALUES (?,?,?,?,?,?,?)"""
        val = (data_source_, search_name_, search_subcategory_, 
                question_reference_, question_results_, date_collected_,
                current_data_)
        cursor.execute(sql, val)
        cursor.commit()
    print("upload_complete")



# Function to return the API key
def get_api_key(key_name, config_section='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(r'Z:/Technology/Data Science/Config/API_config.txt')
    key = config.get(config_section, key_name)
    return key


# List of country codes
# countries = ['GB','FR','DE','IT','ES','PL','RU','CN','IN','JP','PH','ID','KR',
#               'CA','US','BR','MX']

countries = ["ZA"]

# IMF API url
url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'

# Creating a dictionary to store CPI data
dict_ = {'Countries':[], 'Month': [], 'CPI': []}
for c in countries:
    print(c)
    
    # Connecting to CPI database for each country
    key = 'CompactData/CPI/M.{}.PCPI_IX'.format(c)
    
    # Navigate to series in API-returned JSON data
    data = (requests.get(f'{url}{key}').json()['CompactData']['DataSet']['Series']['Obs'])
    
    # for each dictionary in the data get months from 2020 and 2021 and append
    # values to the dictionary
    for vals in data:
        if vals['@TIME_PERIOD'].split('-')[0] in ['2020', '2021']:
            dict_['Countries'].append(c)
            dict_['Month'].append(vals['@TIME_PERIOD'])
            dict_['CPI'].append(vals['@OBS_VALUE'])

    # Timer because the API doesn't like too many calls per second
    sleep(1)


# Changing key for australia as it only has quarterly data for some reason
key = 'CompactData/CPI/Q.AU.PCPI_IX'


# Navigate to series in API-returned JSON data
australia = (requests.get(f'{url}{key}').json()['CompactData']['DataSet']['Series']['Obs'])

# for each dictionary in the data get months from 2020 and 2021 and append
# values to the dictionary
for vals in australia:
    if vals['@TIME_PERIOD'].split('-')[0] in ['2020', '2021']:
        dict_['Countries'].append('AU')
        dict_['Month'].append(vals['@TIME_PERIOD'])
        dict_['CPI'].append(vals['@OBS_VALUE'])


# Create a dataframe from the dictionary
df = pd.DataFrame(data=dict_)


# countries_sql = ['aus', 'brazil', 'canada', 'china', 'france', 'germany',
#                 'india', 'indonesia', 'italy', 'japan', 'south_korea', 'mexico',
#                 'philippines', 'poland', 'russia', 'spain',
#                 'uk', 'us']

countries_sql = ['south_africa']


# country_inits = ['AU', 'BR', 'CA', 'CN', 'FR', 'DE', 'IN', 'ID',
#                   'IT', 'JP', 'KR', 'MX', 'PH', 'PL', 'RU', 'ES', 'GB', 'US']

country_inits = ["ZA"]


print(df)





sql_dfs = []
for c, c_code in zip(countries_sql, country_inits):
    print(c_code)
    cdf = df.loc[df['Countries'] == c_code]
    data_source_ = ['country_tool' for i in range(len(cdf))]
    search_name_ = ['{}_cpi'.format(c) for i in range(len(cdf))]
    search_subcategory_ = cdf['Month'].apply(lambda x: 'CPI ({})'.format(x))
    question_reference_ = cdf['Month'].apply(lambda x: 'ct_{}_i_{}'.format(c, x))
    question_results_ = cdf['CPI']
    date_collected_ = [date.today() for i in range(len(cdf))]
    current_data_ = ['Y' for i in range(len(cdf))]
    
    cols = ['data_source', 'search_name', 'search_subcategory',
            'question_reference', 'question_results', 'date_collected',
            'current_data']
    
    data = [data_source_, search_name_, search_subcategory_, question_reference_,
            question_results_, date_collected_, current_data_]
    
    dict_ = {cols[i]: data[i] for i in range(7)}
    
    sql_df = pd.DataFrame(dict_)
    
    sql_dfs.append(sql_df)

# from: https://fred.stlouisfed.org/series/ARGCPALTT01IXNBM
# argentina = pd.read_csv('Y:/Python/Consumer Price/argentina_cpi.csv',
#                         usecols=[0,1,2,3,4,5,6])
# sql_dfs.append(argentina)
print(sql_dfs[0])

for sql_df in sql_dfs:
    insert_SQL(sql_df)