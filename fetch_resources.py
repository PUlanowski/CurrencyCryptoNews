#!/usr/bin/python3
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
import os

api = KaggleApi()
api.authenticate()

def fetch_ccy():
    kaggle_usr = 'thebasss'
    kaggle_path = 'currency-exchange-rates'
    api.dataset_download_files(kaggle_usr +'/'+ kaggle_path)
    with ZipFile(kaggle_path+'.zip', 'r') as zipObj:
       zipObj.extractall()
    os.remove(kaggle_path+'.zip')

def fetch_crypto_ccy():
    kaggle_usr = 'jessevent'
    kaggle_path = 'all-crypto-currencies'
    api.dataset_download_files(kaggle_usr +'/'+ kaggle_path)
    with ZipFile(kaggle_path+'.zip', 'r') as zipObj:
       zipObj.extractall()
    os.remove(kaggle_path+'.zip')

def fetch_wallstreetbets():
    kaggle_usr = 'shergreen'
    kaggle_path = 'wallstreetbets-subreddit-submissions'
    kaggle_file = 'wallstreetbets_submission.json'
    api.dataset_download_file(kaggle_usr +'/'+ kaggle_path, kaggle_file)

if __name__=="__main__":
    fetch_ccy()
    fetch_crypto_ccy()
    fetch_wallstreetbets()
