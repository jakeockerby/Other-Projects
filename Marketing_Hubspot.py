# -*- coding: utf-8 -*-
"""
Created on Tue May  4 09:02:25 2021

@author: Jake
"""

import requests
import json
import ast
import datetime
import pandas as pd
import numpy as np
from datetime import date
import configparser
import pyodbc
from time import sleep
from tqdm import tqdm
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
pd.set_option('display.max_columns', 100000)
# Initialize the ConfigParser
API_config = configparser.ConfigParser()

# Read the API_config
API_config.read(r'Z:/Technology/Data Science/Config/API_config.txt')
API_key = API_config.get('DEFAULT', 'hubspot_api_key')

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


# # Creating table in database
# create_table = """
# CREATE TABLE t_marketing (
#     archieved varchar(255),
#     created_at date,
#     email varchar(255),
#     firstname varchar(255),
#     lastname varchar(255),
#     hs_lastmodifieddate date,
#     hs_object_id int,
#     hs_owner_id real,
#     latest varchar(255),
#     insert_date date,
#     sector varchar(255),
#     country varchar(255),
#     product varchar(255),
#     mobilephone varchar(255),
#     website varchar(255),
#     lead_source varchar(255),
#     interested_in varchar(255),
# )"""

# cursor.execute(create_table)
# cnxn.commit()



today = str(date.today())


url= 'https://api.hubapi.com/properties/v1/contacts/properties?hapikey=#redacted#'

r=requests.get(url=url)

response = r.text
# print(response)
# print(json.dumps(response,  indent=1))

after = '10000'
def get_data(after):
    url= 'https://api.hubapi.com/crm/v3/objects/contacts'

    querystring = {"limit":"100",
                    "after": after,
                    "archived":"false",
                    "properties": "firstname,lastname,email,createdate,company"
                                    + "jobtitle,sector,country,product," +
                                    "lead_source,hubspot_owner_id,website," +
                                    "interested_in,which_territories_do_you_have_research_needs_for_,mobilephone",                                
                    "hapikey": API_key
                    }

    
    headers = {'accept': 'application/json'}
    
    response = requests.request("GET", url, headers=headers, 
                              params=querystring)

    parsed_data = json.loads(response.text)
    # print(parsed_data)
    #print(json.dumps(parsed_data, indent = 4, sort_keys=True))
           
    return parsed_data

nan = np.nan
rows = []


def build_data_frame(parsed, date):
    i=0
    try:
        for i in range(100):
            try:
                archieved = (parsed['results'][i]['archived'])
                
                createdAt = (parsed['results'][i]['createdAt'])
                createdAt = createdAt.split("T", 1)
                createdAt_date = createdAt[0]
                
                createdAt_minus1 = (parsed['results'][i-1]['createdAt'])
                createdAt_minus1 = createdAt_minus1.split("T", 1)
                createdAt_minus1_date = createdAt_minus1[0]
                
                createdate = (parsed['results'][i]['properties']['createdate'])
                createdate = createdate.split("T", 1)
                created_at_date = createdate[0]
                email = (parsed['results'][i]['properties']['email'])
                firstname = (parsed['results'][i]['properties']['firstname'])
                
                
                try: 
                    hs_lastmodifieddate = (parsed['results'][i]['properties'][
                        'lastmodifieddate'])
                    hs_modified = hs_lastmodifieddate.split("T", 1)
                    modified_date = hs_modified[0]
                except KeyError:
                    modified_date = ''
                hs_object_id = (parsed['results'][i]['properties']
                                ['hs_object_id'])
                lastname = (parsed['results'][i]['properties']['lastname'])
                latest_val = 'Y'
                insert_date = date
                try:
                    hs_owner_id = (parsed['results'][i]['properties']
                                    ['hubspot_owner_id'])
                except KeyError:
                    hs_owner_id = ''
                #jobtitle = (parsed['results'][i]['properties']
                  #                  ['jobtitle'])
                sector =  (parsed['results'][i]['properties']
                                    ['sector'])
                country = (parsed['results'][i]['properties']
                                    ['country'])
                product = (parsed['results'][i]['properties']
                                    ['product'])
                lead_source = (parsed['results'][i]['properties']
                                    ['lead_source'])
                interested_in = (parsed['results'][i]['properties']
                                    ['which_territories_do_you_have_research_needs_for_'])
                website = (parsed['results'][i]['properties']
                                    ['website'])
                mobile_phone = (parsed['results'][i]['properties']
                                    ['mobilephone'])
                rows.append({'archieved': archieved,
                                    'createdAt': createdAt_date,
                                    'email': email,
                                    'firstname': firstname,
                                    'lastname':  lastname,
                                    'hs_lastmodifieddate': modified_date,
                                    'hs_object_id': hs_object_id,
                                    'hs_owner_id':hs_owner_id,
                                    'latest' : latest_val,
                                    'insert_date' : insert_date,
                                    'hs_owner_id': hs_owner_id,
                                    #'job title':jobtitle,
                                    'sector':sector,
                                    'country':country,
                                    'product':product,
                                    'mobilephone':mobile_phone,
                                    'website':website,
                                    'lead_source':lead_source,
                                    'interested_in':interested_in
                                    })
                df = pd.DataFrame(rows)
                
                
            except UnboundLocalError:
                print('error')
                after_val = 0
                return(df, after_val) 
    except IndexError: 
        pass        
    try:
        after_val = (parsed['paging']['next']['after'])
        #print(after_val)
        df = df.drop_duplicates()
        return(df, after_val)
    
    except KeyError:
        pass
        after_val = 0
        return(df, after_val)
    
    
    
def add_records(df, after):
    try:
        today = str(date.today())
        parsed_data = get_data(after)
        parsed, after_val = build_data_frame(parsed_data, today)
    
        for i in tqdm(range(500)):
            if parsed['createdAt'].tail(1).values[0] != today:
                api_data = get_data(after_val)
                parsed, after_val = build_data_frame(api_data, today)
                after_val = int(after_val)
                print(parsed['createdAt'].tail(1).values[0])
                print(today)
            else:
                print('End of the records')
            
        parsed['createdAt'] = pd.to_datetime(parsed['createdAt'])
        parsed['createdAt'] = parsed['createdAt'].dt.date
        parsed = parsed.sort_values(by='createdAt')
        parsed['date'] = parsed['createdAt']
        parsed = parsed.set_index(['date'])
        parsed['hs_owner_id'] = parsed['hs_owner_id'].fillna(0.0)
        parsed['firstname'] = parsed['firstname'].fillna('null')
        parsed['lastname'] = parsed['lastname'].fillna('null')
        parsed['email'] = parsed['email'].fillna('null')
        parsed['website'] = parsed['website'].fillna('null')
        
        # Insert each row in dataframe into database table
        for index, row in parsed.iterrows():
            insert_query = """INSERT INTO [dbo].[marketing]
              VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}',
                      '{12}', '{13}', '{14}', '{15}', '{16}')
            """.format(row['archieved'], row['createdAt'],
                        row['email'].replace("'", "''"), 
                        row['firstname'].replace("'", "''"),
                        row['lastname'].replace("'", "''"),
                        row['hs_lastmodifieddate'], row['hs_object_id'],
                        row['hs_owner_id'], row['latest'], row['insert_date'],
                        row['sector'], row['country'], row['product'],
                        row['mobilephone'], row['website'].replace("'", "''"), 
                        row['lead_source'], row['interested_in'])

            cursor.execute(insert_query)
            cnxn.commit()
        
        df = df.append(parsed)
        df = df.sort_values(by='createdAt')
        df.to_csv('marketing.csv', index=False)
    except:
        print('End of the records')
    
    return df
        

# Get all records and store in csv file
# if __name__ == "__main__":
#     api_data = get_data(0)
#     # print(api_data)
#     parsed, after_val = build_data_frame(api_data, today)
#     # print(parsed)
#     # after_val = int(after_val)
#     for i in tqdm(range(500)):
#         try:
#             api_data = get_data(after_val)
#             parsed, after_val = build_data_frame(api_data, today)
#             after_val = int(after_val)
#         except ValueError:
#             print('End of the records')
#             pass
        
            
            
#     print('ALL DONE!')
    
#     parsed.to_csv('marketing.csv', index=False)
    
# Read csv file and get data from dates you want
hubspot_data = pd.read_csv('Y:/Python/hubspot/marketing.csv')
hubspot_data['createdAt'] = pd.to_datetime(hubspot_data['createdAt'])
hubspot_data['createdAt'] = hubspot_data['createdAt'].dt.date
hubspot_data = hubspot_data.sort_values(by='createdAt')
hubspot_data['date'] = hubspot_data['createdAt']
hubspot_data = hubspot_data.set_index(['date'])
hubspot_data['hs_owner_id'] = hubspot_data['hs_owner_id'].fillna(0.0)
hubspot_data['firstname'] = hubspot_data['firstname'].fillna('null')
hubspot_data['lastname'] = hubspot_data['lastname'].fillna('null')
hubspot_data['email'] = hubspot_data['email'].fillna('null')
hubspot_data['website'] = hubspot_data['website'].fillna('null')
# test = hubspot_data.head(10000)
# print(test)
print(hubspot_data)


# Select dates you want
# select_dates = hubspot_data['2020-01-01':'2021-01-01']
# print(select_dates)


# Append new records to the full dataset
new_after = hubspot_data['hs_object_id'].tail(1).values[0]
new_df = add_records(hubspot_data, new_after)


# # Insert each row in dataframe into database table
# for index, row in new_df.iterrows():
#     insert_query = """INSERT INTO [dat].[t_marketing]
#       VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}',
#               '{12}', '{13}', '{14}', '{15}', '{16}')
#     """.format(row['archieved'], row['createdAt'],
#                 row['email'].replace("'", "''"), 
#                 row['firstname'].replace("'", "''"),
#                 row['lastname'].replace("'", "''"),
#                 row['hs_lastmodifieddate'], row['hs_object_id'],
#                 row['hs_owner_id'], row['latest'], row['insert_date'],
#                 row['sector'], row['country'], row['product'],
#                 row['mobilephone'], row['website'].replace("'", "''"), 
#                 row['lead_source'], row['interested_in'])

#     cursor.execute(insert_query)
#     cnxn.commit()