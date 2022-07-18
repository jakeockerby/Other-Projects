# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 13:05:37 2021

@author: Jake
"""

import numpy as np
import pandas as pd
import configparser
import pyodbc
import json
import requests
pd.set_option('display.max_rows', 635)
#Initialize the ConfigParser
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


# Function to return the API key
def get_api_key(key_name, config_section='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(r'Z:/Technology/Data Science/Config/API_config.txt')
    key = config.get(config_section, key_name)
    return key
key = get_api_key('us_census_api_key')

# 1 is boys, 2 is girls
data_list=[]

for i in [1,2]:
    url = 'https://api.census.gov/data/timeseries/idb/1year?get=NAME,AGE,POP,AREA_KM2&YR=2021&SEX={0}&key={1}'.format(i, key)
    response = requests.get(url)
    country_data = eval(response.text)
    pd.set_option('display.max_rows', 50)
    regions = ['Australia', 'Brazil', 'Canada', 'China', 'France', 'Germany', 'India', 'Indonesia', 'Italy', 'Japan', 'Korea, South', 'Mexico', 'Philippines', 'Poland', 'Russia', 'Spain', 'United Kingdom', 'United States']
    country_df = pd.DataFrame(data=country_data[1:], columns=country_data[0])
    country_df = country_df.loc[country_df['NAME'].isin(regions)]
    country_df['AGE'] = country_df['AGE'].astype(int)
    country_df['POP'] = country_df['POP'].astype(int)
    # print(country_df['POP'])
    country_df = country_df.loc[(country_df['AGE']>=3)&(country_df['AGE']<=18)]
    country_df['SEX'] = country_df['SEX'].apply(lambda x: x.replace(str(i), 'Boys') if i ==1 else x.replace(str(i), 'Girls'))
    country_df = country_df.drop('AREA_KM2', axis=1)
    data_list.append(country_df)
df = pd.concat(data_list)
df_json = (df.to_json(orient='records'))
data = json.loads(df_json)

#calculating the sum of countries for all the ages for boys and girls.

# countries_sum = {'Australia' : 0, 'Brazil' : 0, 'Canada' : 0, 'China' : 0 ,'France' : 0 ,'Germany' :0, 'India' : 0, 'Indonesia' : 0, 'Italy' : 0, 'Japan': 0, 'South Korea' :0, 'Mexico': 0, 'Philippines' : 0, 'Poland' : 0, 'Russia' : 0, 'Spain' : 0, 'UK' : 0, 'USA':0}
countries_sum = {}
for datum in data:
  # print(datum)
  if datum['NAME'] in countries_sum:
    countries_sum[datum['NAME']] =  countries_sum[datum['NAME']] + datum['POP']
  else:
    countries_sum[datum['NAME']] =  datum['POP']
# print(countries_sum)

#reading csv file to calucate region population
New_df= pd.read_csv(r"C:\Users\Jake\Downloads\population_by_region_1.csv")
updated_df= New_df.dropna(axis=1)
# updated_df_1= updated_df.head(20)

# new_json = (updated_df.to_json(orient='records'))
# new_data = json.loads(new_json)
#get_data=list(map(lambda dic: {dic['Row Labels']: dic['Sum of Population']}, new_data))



regions= ['Kids Australia',
  'Kids Brazil',
  'Kids Canada',
  'Kids China',
  'Kids Germany',
  'Kids Spain',
  'Kids France',
  'Kids UK',
  'Kids India',
  'Kids Italy',
  'Kids Indonesia',
  'Kids Japan',
  'Kids South Korea',
  'Kids Mexico',
  'Kids Philippines',
  'Kids Poland',
  'Kids Russia',
  'Kids USA',
  'Grand Total']


territories = [x for x in list(updated_df['Row Labels'].unique()) if x not in regions]

updated_df = updated_df.loc[updated_df['Row Labels'].isin(territories)]
# print(updated_df)
updated_df['Sum of Population '] = updated_df['Sum of Population '].apply(lambda x: float(x.split('%')[0])/100)



prods_query = """SELECT TOP (325) [product]
      ,[product_id]
      ,[survey_location]
      ,[portal_location]
  FROM [dat].[t_locations] """


# def f(x):
#     return list(prods['product'].loc(p))

Query = pd.read_sql(prods_query, cnxn)
prods = pd.DataFrame(Query)

prods['portal_location'] = prods['portal_location'] + ',' + prods['product']
# print(prods['portal_location'])
# updated_df['Row Labels'] = updated_df['Row Labels'].apply(lambda x: x + ','+ f(x))
updated_df = updated_df.head(244)

updated_df['Products'] = updated_df['Products'].apply(lambda x: x.replace("Kids ", ''))
updated_df['Products'] = updated_df['Products'].apply(lambda x: x.replace("UK", 'United Kingdom'))
updated_df['Products'] = updated_df['Products'].apply(lambda x: x.replace("USA", 'United States'))
updated_df['Products'] = updated_df['Products'].apply(lambda x: x.replace("South Korea", 'Korea, South'))


#print(updated_df['Row Labels'].loc[updated_df['Products'] == 'United Kingdom'].unique())




terr_dfs = []
for t, s, c in zip(list(updated_df['Row Labels']), list(updated_df['Sum of Population ']), list(updated_df['Products'])):
    terr_df = df.loc[df['NAME'] == c]
    terr_df['LOC'] = t
    terr_df['POP'] = terr_df['POP'].apply(lambda x: round(x*s))
    terr_dfs.append(terr_df)
    
    
final_df = pd.concat(terr_dfs)
print(final_df)

writer = pd.ExcelWriter('pandas_simple.xlsx', engine='xlsxwriter')
final_df.to_excel(writer, sheet_name='Sheet1')
writer.save()