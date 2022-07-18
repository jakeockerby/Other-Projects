# -*- coding: utf-8 -*-
"""
Created on Fri Apr 30 10:45:17 2021

@author: Jake
"""
import numpy as np
import pandas as pd
import configparser
import pyodbc
import json
import requests

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

# 0 is combined, 1 is boys, 2 is girls

# Function to return the API key
def get_api_key(key_name, config_section='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(r'Z:/Technology/Data Science/Config/API_config.txt')
    key = config.get(config_section, key_name)
    return key

key = get_api_key('us_census_api_key')

for i in [1,2]:
    url = 'https://api.census.gov/data/timeseries/idb/1year?get=NAME,AGE,POP,AREA_KM2&YR=2021&SEX={0}&key={1}'.format(i, key)
    response = requests.get(url)
    country_data = eval(response.text)
    
    pd.set_option('display.max_rows', 500)
    regions = ['Poland']
    country_df = pd.DataFrame(data=country_data[1:], columns=country_data[0])
    country_df = country_df.loc[country_df['NAME'].isin(regions)]
    country_df['AGE'] = country_df['AGE'].astype(int)
    country_df['POP'] = country_df['POP'].astype(int)
    country_df = country_df.loc[(country_df['AGE'] <= 18)]
    country_df['SEX'] = country_df['SEX'].apply(lambda x: x.replace(str(i), 'Male') if i ==1 else x.replace(str(i), 'Female'))
    country_df = country_df.drop('AREA_KM2', axis=1)
    print(country_df)
    # print(country_df['POP'].sum())
    # print(100*(country_df['POP'].sum() / 38185913))
    
    # with pd.ExcelWriter('census_dem_{}.xlsx'.format(i)) as writer:  
    #     country_df.to_excel(writer, sheet_name='Sheet1', 
    #                         startrow=4, startcol=3)
# acs = cen.products.ACS()

# c = Census(get_api_key('us_census_api_key'))
# pop = c.data(year=2019)

# print(pop)


# # -- Read locations table from mysql database and put in dataframe -- 
# df = df.sort_values(by='location')
# orig_df = orig_df.loc(orig_df['locations'].isin(list1))
# list1 = sorted(list1)

# location_dfs = []
# for loc in list(df['location'].unique()):
#     ldf = df.loc[df['location'] == loc]
    
#     # Getting country that belongs to each set of locations
#     country = list(ldf['product'].unique())[0]
    
#     # Get population for correct country
#     country_pop = orig_df['population'].loc[orig_df['product'] == country].values
    
#     for i i
    