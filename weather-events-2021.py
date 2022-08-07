import glob
import logging
import os
import re
from tracemalloc import start
from bs4 import BeautifulSoup
from datetime import datetime
import requests as r

import pandas as pd


URL = r'https://en.wikipedia.org/wiki/Weather_of_2021'


def scrape(url):
    month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    response = r.get(url)
    if response.status_code != r.codes.ok:
        print('Couldn\'t connect')

    doc = BeautifulSoup(response.text, 'lxml')

    frame = doc.find('div', id='mw-content-text')
    for i in range(1, 16):
        data_list = frame.find_all('ul')
    
    entered_records = []
    with open('extracted_weather_wikipedia.tmp', 'a') as f:
        f.write('DATE;EVENT\n')
        for month in month_names:
            for item in data_list[3:17]:
                item = item.find_all('li')
                for i in item:
                    hyphen_index = i.text[:60].rfind('–')
                    dates = i.text[:hyphen_index].strip()
                    description = i.text[hyphen_index+1:].strip()

                    if i in entered_records:
                        continue
                    text = i.text
                    if (text.startswith(month) \
                    or '2020' in text[:(len(text) // 2)]):
                        f.write(dates + ';' + description + '\n')
                        entered_records.append(i)

    with open('extracted_weather_wikipedia.tmp', 'r') as f:
        for lines in f.readlines():
            if ';' in lines:
                with open('reformed_weather_wikipedia.csv', 'a') as g:
                    print(lines)
                    g.write(lines)
    
    return 

def extract_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process, sep=';')
    return dataframe


def extract(dataset):    
    df = pd.DataFrame(columns=['DATE', 'EVENT'])
    
    for csvfile in glob.glob('reformed_weather_wikipedia.csv'):
        df = df.append(extract_csv(csvfile), ignore_index=True)

    return df


def transform(dataframe):
    pass


def load(data_to_load, targetfile):
    data_to_load.to_csv(targetfile)


def log(message):
    pass


dataset_path = scrape(URL)
extracted_data = extract(dataset_path)
# transformed_data = transform(extracted_data)
load(extracted_data, 'weather-events-2021.csv')

