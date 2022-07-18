# -*- coding: utf-8 -*-
"""
Created on Fri Aug 20 12:37:08 2021

@author: Jake
"""

import wbdata
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import configparser
import pyodbc
from fuzzywuzzy import process
from datetime import date, timedelta
import mysql


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





# # Initialize the ConfigParser
# config = configparser.ConfigParser()

# # Read the config
# #config.read('/home/portaladmin/scripts/python/Data_Science/config')
# config.read(r'Z:/Technology/Data Science/Config/config.txt')

# #config to get the MYSQL info
# P4mysqlusername = config.get('DEFAULT', 'Portal4_mysqlusername')
# P4mysqlpassword = config.get('DEFAULT', 'Portal4_mysqlpassword')
# P4mysqlhost = config.get('DEFAULT', 'Portal4_mysqlhost')
# P4mysqldatabase = 'portal_IFDEV01'

# #-----Portal 4 Configurations whilst running in parallel-----#
# #MYSQL Connections Setup
# p4mydb = mysql.connector.connect(
#   #host="194.39.166.179",
#   host = P4mysqlhost,
#   user = P4mysqlusername,
#   password = P4mysqlpassword,
#   database = P4mysqldatabase
# )

# #print(mydb)
# P4mycursor = p4mydb.cursor()



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



# def insert_mySQL(df):
#     for row in df.itertuples():
#         data_source_ = row.data_source
#         search_name_ = row.search_name
#         search_subcategory_ = row.search_subcategory
#         question_reference_ = row.question_reference
#         question_results_ = row.question_results
#         date_collected_ = row.date_collected
#         current_data_ = row.current_data

#         mysql_ = """INSERT INTO `api_data` (data_source,
#         search_name, search_subcategory, question_reference,
#         question_results, date_collected, current_data)
#         VALUES (%s,%s,%s,%s,%s,%s,%s)"""
#         val = (data_source_, search_name_, search_subcategory_, 
#                 question_reference_, question_results_, date_collected_,
#                 current_data_)
        
#         P4mycursor.execute(mysql_, val)
#         p4mydb.commit()
#     print("upload_complete")


# Function to convert dataframe into suitable format
def df_formatter(df):
    dfu = df.unstack(level=0).reset_index().rename(columns={'': 'year'})
    data = [list(dfu.iloc[i,:].values) for i in range(len(list(dfu.index)))]
    names = [list(dfu.columns)[i][-1] for i in range(len(list(dfu.columns)))]
    
    
    new_df = pd.DataFrame(data=data, columns=names)
    new_df['year'] = new_df['year'].astype(int)
    new_df = new_df.loc[new_df['year'] >= 2000]
    new_df = new_df.fillna(0.00)
    
    return new_df


# Products
# countries = ["GB","FR","DE","ES","IT","PL","RU","US","CA","MX","BR","AU","IN",
#              "CN","KR","JP","PH","ID"]

countries = ["ZA", "GB"]

# countries_sql = ['aus', 'brazil', 'canada', 'china', 'france', 'germany',
#                 'india', 'indonesia', 'italy', 'japan', 'south_korea', 'mexico',
#                 'philippines', 'poland', 'russia', 'spain',
#                 'uk', 'us']

countries_sql = ['south_africa', 'uk']

# match_dict = {'aus': 'Australia', 'brazil': 'Brazil', 'canada': 'Canada',
#               'china': 'China', 'france': 'France', 'germany': 'Germany',
#               'india': 'India', 'indonesia': 'Indonesia', 'italy': 'Italy',
#               'japan': 'Japan', 'south_korea': 'Korea, Rep.', 'mexico': 'Mexico',
#               'philippines': 'Philippines', 'poland': 'Poland',
#               'russia': 'Russian Federation', 'spain': 'Spain',
#               'uk': 'United Kingdom', 'us': 'United States'}

match_dict = {'south_africa': 'South Africa', 'uk': 'United Kingdom'}

# # Link to inflation data
# inflation = {'NY.GDP.DEFL.KD.ZG':'Annual Inflation %'}

# # Get annual inflation %
# inf_df = wbdata.get_dataframe(inflation, country=countries, convert_date=False)

# new_inf_df = df_formatter(inf_df)
# print(new_inf_df)

# for c in list(new_inf_df.columns)[1:]:
#     graph = sns.catplot(x='year', y=c, data=new_inf_df, kind='bar', height=6,
#                         aspect=2)
#     graph.savefig('{}_inflation.png'.format(c))

# sql_dfs = []
# for c in countries_sql:
#     data_source_ = ['country_tool' for i in range(21)]
#     search_name_ = ['{}_i'.format(c) for i in range(21)]
#     search_subcategory_ = new_inf_df['year'].apply(lambda x: 'Annual Inflation % {}'.format(x))
#     question_reference_ = new_inf_df['year'].apply(lambda x: 'ct_{}_i_{}'.format(c, x))
#     c_match = process.extractOne(c, list(new_inf_df.columns))[0]
#     question_results_ = new_inf_df[c_match].apply(lambda x: round(x, 2))
#     date_collected_ = [date.today() for i in range(21)]
#     current_data_ = ['Y' for i in range(21)]
    
#     cols = ['data_source', 'search_name', 'search_subcategory',
#             'question_reference', 'question_results', 'date_collected',
#             'current_data']
    
#     data = [data_source_, search_name_, search_subcategory_, question_reference_,
#             question_results_, date_collected_, current_data_]
    
#     dict_ = {cols[i]: data[i] for i in range(7)}
    
#     sql_df = pd.DataFrame(dict_)
    
#     sql_dfs.append(sql_df)
    
# print(sql_dfs[0])

# for sql_df in sql_dfs:
#     insert_SQL(sql_df)








# Link to GDP per capita ($USD) by year
gdp_per_capita = {'NY.GDP.PCAP.CD':'GDP per capita (Current $USD)'}


# Get GDP data
gdp_cap = wbdata.get_dataframe(gdp_per_capita, country=countries, convert_date=False)
new_gdp_cap = df_formatter(gdp_cap)
print(new_gdp_cap)

# for c in list(new_gdp_cap.columns)[1:]:
#     graph = sns.catplot(x='year', y=c, data=new_gdp_cap, kind='bar', height=6,
#                         aspect=2)
#     graph.savefig('{}_gdp_capita.png'.format(c))
    
    

sql_dfs = []
for c in countries_sql:
    data_source_ = ['country_tool' for i in range(21)]
    search_name_ = ['{}_g'.format(c) for i in range(21)]
    search_subcategory_ = new_gdp_cap['year'].apply(lambda x: 'GDP per capita ({})'.format(x))
    question_reference_ = new_gdp_cap['year'].apply(lambda x: 'ct_{}_g_{}'.format(c, x))
    c_match = match_dict[c]
    question_results_ = new_gdp_cap[c_match].apply(lambda x: round(x, 2))
    date_collected_ = [date.today() - timedelta(days=2) for i in range(21)]
    current_data_ = ['Y' for i in range(21)]
    
    cols = ['data_source', 'search_name', 'search_subcategory',
            'question_reference', 'question_results', 'date_collected',
            'current_data']
    
    data = [data_source_, search_name_, search_subcategory_, question_reference_,
            question_results_, date_collected_, current_data_]
    
    dict_ = {cols[i]: data[i] for i in range(7)}
    
    sql_df = pd.DataFrame(dict_)
    
    sql_dfs.append(sql_df)
    
print(sql_dfs[-1])
insert_SQL(sql_dfs[0])
# for sql_df in sql_dfs:
#     insert_SQL(sql_df)


# insert_mySQL(sql_dfs[-2])
# insert_mySQL(sql_dfs[-1])