# -*- coding: utf-8 -*-
"""
Created on Wed Oct 13 09:32:52 2021

@author: Jake
"""

from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
import numpy as np
import requests
import mechanize
import http.cookiejar
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import matplotlib.pyplot as plt
from time import sleep
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import urllib
from selenium.webdriver import ActionChains
import csv
import timeit



# # Read contacts sheet
# data = pd.read_excel('Y:\Python\MIPcom\Book1.xlsx', engine='openpyxl')
# data = data.rename(columns={'Unnamed: 0': 'email', 'Unnamed: 1': 'phone', 'Unnamed: 2': 'linkedin', 'Unnamed: 3': 'other_contact'})
# data = data.fillna('null')
# print(data)

# # Arrange contacts into correct order
# contact_dfs = []
# for row in data.itertuples():
#     dict_ = {'email': [], 'phone': [], 'linkedin': [], 'other_contacts':[]}
#     for i in row[1:]:
#         if '@' in i:
#             dict_['email'].append(i)
#         elif 'tel:' in i:
#             dict_['phone'].append(i)
#         elif 'linkedin' in i:
#             dict_['linkedin'].append(i)
#         else:
#             if dict_['other_contacts'] == []:
#                 dict_['other_contacts'].append(i)
    
#     if len(dict_['email']) > 1:
#         dict_['email'] = dict_['email'][0]
            
#     for key in list(dict_.keys()):
#         if dict_[key] == []:
#             dict_[key].append('null')
            
    
#     contact_df = pd.DataFrame(data=dict_)
#     contact_dfs.append(contact_df)
    
# df = pd.concat(contact_dfs, ignore_index=True)
# print(df)

# # print to excel
# df.to_excel('sorted_contacts.xlsx')




# Start stopwatch
start = timeit.default_timer()


chrome_options = Options()
chrome_options.add_argument("--headless")

# Open the text file with the login info
f = open("login.txt", newline='')
csv_reader = csv.reader(f)
username = next(csv_reader)
password = next(csv_reader)
f.close()

# Change this information with your own directory
download_dir = "Y:\Python\MIPcom\chromedriver.exe"
driver = webdriver.Chrome(executable_path=download_dir, options=chrome_options)
# driver = webdriver.Chrome(executable_path=download_dir)
url = r'https://www.mipcom.com/en-gb/portal/participant-portal/participant-directory.html'

# Navigate to the url
driver.get(url)

# Send the username and password keys to the correct places
Username = driver.find_element_by_id('username')
Password = driver.find_element_by_id('password')

Username.send_keys(username)
Password.send_keys(password)
Password.send_keys(Keys.ENTER)
sleep(7)


# Click the cookies button
cookies_btn = driver.find_element_by_id('onetrust-reject-all-handler')
cookies_btn.click()


msgs = driver.find_element_by_xpath('//*[@class="conversation-list-toolbar theme__ac-color--background-color"]/div[3]')
msgs.click()

# Get  the scroll height
last_height = driver.execute_script("return document.body.scrollHeight")

# Page count
page_c = 0
while True:
    page_c += 1
    # Scroll down to the bottom
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Wait to load the page
    sleep(10)

    # Calculate new scroll height and compare with last scroll height
    new_height = driver.execute_script("return document.body.scrollHeight")

    if new_height == last_height:

        break

    last_height = new_height
    
    print('Page {} Loaded!'.format(page_c))


strainer = SoupStrainer(class_='filter-results')
page_source = driver.page_source
soup = BeautifulSoup(page_source, 'lxml', parse_only=strainer)

names = soup.find_all('h3')
names_list = []
for name in names:
    names_list.append(name.text)

# names_list = names_list[5293:]
print(len(names_list))

# Get the links to each participant profile
name_link = driver.find_elements_by_xpath('//*[@class="participant-name text-primary"]/h3')
# name_link = name_link[:5]

# Making a dictionary to store the participant data
participants = {}

# Setting up a count to track progress
c = 5292
# For each name in the link, scrape the relevent info
for i in range(len(names_list)):
    c+=1
    
    
    search_bar = driver.find_element_by_xpath('//*[@class="search-bar btn-border-radius"]/input')    
    search_bar.send_keys(names_list[i])
    search_bar.send_keys(Keys.ENTER)
    sleep(1)

    # Get new name links on each loop as session state changes
    name_link = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,'//*[@class="participant-name text-primary"]/h3')))
    
    country = driver.find_element_by_class_name('participant-country').text
    # print(country)
    try:
        driver.execute_script("arguments[0].click();", name_link)
        # sleep(7)
    except:
        name_link.click()
        # sleep(7)
    
    sleep(1)
    
    # Strainer to save some time
    try:
        text_ = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'participant-profile-content')))
        strainer = SoupStrainer(class_='participant-profile-content')
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml', parse_only=strainer)
        # print(text_.text.split('\n'))
        
        # name = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'participant-name')))
        # print('waited')
        name_title = soup.find('h1')
        job = soup.find('h2')
        org = soup.find('h3')
        
        print(name_title.text)
        # sleep(5)
        
        # Update the dictionary
        participants.update({name_title.text: {'Job Title': [], 'Organisation': [],
                    'Participant Country': []}})
        
        participants[name_title.text]['Job Title'].append(job.text)
        participants[name_title.text]['Organisation'].append(org.text)
        participants[name_title.text]['Participant Country'].append(country)
        
        # Get category info
        cats = soup.find_all(class_='form-group-view-mode')
        
        for cat in cats:
            title = cat.find('h4').text
            info = cat.find_all('span')
            # if info == None:
            #     info = info['span']
                
            participants[name_title.text].update({title: []})
            for i in info:
                participants[name_title.text][title].append(i.text)
        
        
        # Find contact info
        contacts = soup.find(class_='participant-contact right')
    
        participants[name_title.text].update({'Contacts': []})
        links = contacts.find_all('a')
        for link in links:
            participants[name_title.text]['Contacts'].append(link['href'])
            
            
        print(c)
        
        # Go back to the previous page
        driver.back()
        # driver.execute_script("window.scrollTo(0, 200);")
        sleep(0.5)
        
        search_bar = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,'//*[@class="search-bar btn-border-radius"]/input')))
        search_bar.clear()
    except:
        # Go back to the previous page
        driver.back()
        # driver.execute_script("window.scrollTo(0, 200);")
        sleep(10)
        
        search_bar = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,'//*[@class="search-bar btn-border-radius"]/input')))
        search_bar.clear()
        continue
    
    


# # Make a new strainer
# page_source = driver.page_source
# strainer = SoupStrainer('div', class_='participant-details')
# soup = BeautifulSoup(page_source, 'lxml', parse_only=strainer)


    
# Lists to store names and dataframes    
names = []
frames = []

# Append each name and associated data to the names and frames lists
for n, d in participants.items():
    names.append(n)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

# Concatenate all the client dataframes
df = pd.concat(frames, keys=names)

# Merge the various columns into one split with a comma
df['Info'] = df[df.columns].apply(
    lambda x: ', '.join(x.dropna().astype(str)),
    axis=1
)

# We can now drop all other columns
df = df['Info']

# Unstack to remove multi-index
df = df.unstack()

# Save final dataframe with all clients in an excel file
df.to_excel('mipcom_clients2.xlsx')
print(df)


# End stopwatch
stop = timeit.default_timer()

# Print runtime
print('Time: {} seconds'.format(stop - start))