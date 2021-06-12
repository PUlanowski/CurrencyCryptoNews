#!/usr/bin/python3
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
import os
import logging

api = KaggleApi()
api.authenticate()

def fetch_ccy():
    """
    This function reaches dataset through Kaggle API.
    :return: .csv file saved in project folder
    """
    kaggle_usr = 'thebasss'
    kaggle_path = 'currency-exchange-rates'
    api.dataset_download_files(kaggle_usr +'/'+ kaggle_path)
    with ZipFile(kaggle_path+'.zip', 'r') as zipObj:
       zipObj.extractall()
    os.remove(kaggle_path+'.zip')

def fetch_crypto_ccy():
    """
    This function reaches dataset through Kaggle API.
    :return: .csv file saved in project folder
    """
    kaggle_usr = 'jessevent'
    kaggle_path = 'all-crypto-currencies'
    api.dataset_download_files(kaggle_usr +'/'+ kaggle_path)
    with ZipFile(kaggle_path+'.zip', 'r') as zipObj:
       zipObj.extractall()
    os.remove(kaggle_path+'.zip')

def fetch_wallstreetbets():
    """
    This function reaches dataset through Kaggle API.
    :return: .json file saved in project folder
    """
    kaggle_usr = 'shergreen'
    kaggle_path = 'wallstreetbets-subreddit-submissions'
    kaggle_file = '../wallstreetbets_submission.json'
    api.dataset_download_file(kaggle_usr +'/'+ kaggle_path, kaggle_file)
    with ZipFile(kaggle_file+'.zip', 'r') as zipObj:
       zipObj.extractall()
    os.remove(kaggle_file+'.zip')

def check_resources():
    """
    This quality check verify if fetch resources are not empty files.
    :return: print check to console and log
    """
    files = ['ccy_map.csv',
             'crypto-markets.csv',
             'currency_exchange_rates_02-01-1995_-_02-05-2018.csv',
             'wallstreetbets_submission.json']
    for file in files:
        if os.stat(file).st_size == 0:
            print ('file', file, 'empty')
            logging.warning('file', file, 'empty')
        else:
            print('file', file, 'not empty')
            logging.info('file', file, 'not empty')



if __name__=="__main__":
    fetch_ccy()
    fetch_crypto_ccy()
    fetch_wallstreetbets()
    check_resources()