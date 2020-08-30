# import libraries
from splinter import Browser
from splinter.exceptions import ElementDoesNotExist
from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime
import json

import pandas as pd 
import numpy as np
from sqlalchemy import create_engine



def scrape_for_ipos():

    # Bring in ipo table from sql
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/IPO_tracker')
    connection = engine.connect()
    sql_ipo_df = pd.read_sql("SELECT * FROM ipo", connection)
    sql_ipo_df.head()

    #############################
    # IPO Scoop Upcoming IPOs
    #############################
    # IPO Scoop Upcoming IPOs
    url = 'https://www.iposcoop.com/ipo-calendar/'
    data = pd.read_html(url)
    ipo_scoop_upcoming_df = data[0]

    # exception handling if no entries are in upcoming IPO table
    if ipo_scoop_upcoming_df["Company"][0] == "No entries were found.":
        ipo_scoop_upcoming_df = ipo_scoop_upcoming_df.drop([0], axis=0)
        
    # if table has values, process updates to dataframe
    else:
        # rename symbol proposed with symbol
        ipo_scoop_upcoming_df.rename(columns={'Symbol proposed':'Symbol'}, inplace=True)
        ipo_scoop_upcoming_df.head()

        # replace 'week of' text from expected to trade column if present
        ipo_scoop_upcoming_df['Expected to Trade'] = ipo_scoop_upcoming_df['Expected to Trade'].str.replace(' Week of', '')
        ipo_scoop_upcoming_df

        # split expected trade date to date and day of week
        ipo_scoop_upcoming_df[['Offer Date','Expected Trade Weekday']] = ipo_scoop_upcoming_df['Expected to Trade'].str.split(' ',expand=True)

        # add date type column to differentiate confirmed vs expected
        ipo_scoop_upcoming_df['date_type'] = "Expected"
        ipo_scoop_upcoming_df['source'] = "IPO Scoop"


    #############################                                
    # IPO Scoop Recent IPOs
    #############################
    url = 'https://www.iposcoop.com/last-100-ipos'
    data = pd.read_html(url)

    ipo_scoop_recent_df = data[0]

    # add date type column to differentiate confirmed vs expected
    ipo_scoop_recent_df['date_type'] = "Confirmed"
    ipo_scoop_recent_df['source'] = "IPO Scoop"

    ipo_scoop_recent_df = ipo_scoop_recent_df.rename(columns={"Symbol": "symbol", "Company": "company", "Offer Date": "offer_date"})


    #############################                                
    # Nasdaq Setup
    #############################

    current_year_month = datetime.today().strftime('%Y-%m')
   
    # scrape nasdaq https://api.nasdaq.com/api/ipo/calendar?date=2020-08
    # note, had to create headers due to time out, solution found here: https://stackoverflow.com/questions/46862719/pythons-requests-library-timing-out-but-getting-the-response-from-the-browser
    url = f'https://api.nasdaq.com/api/ipo/calendar?date={current_year_month}'
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15","Accept-Language": "en-gb","Accept-Encoding":"br, gzip, deflate","Accept":"test/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Referer":"http://www.google.com/"}

    response = requests.get(url, headers=headers)
    data = response.text
    data = json.loads(data)


    #############################                                
    # Nasdaq Priced IPOs
    #############################

    priced_ipos = data["data"]["priced"]["rows"]

    symbol_list = []
    company_list = []
    offer_date_list = []
    market_cap_list = []

    for x in range(len(priced_ipos)):
        symbol_list.append(priced_ipos[x]["proposedTickerSymbol"])
        company_list.append(priced_ipos[x]["companyName"])
        offer_date_list.append(priced_ipos[x]["pricedDate"])
        market_cap_list.append(priced_ipos[x]["dollarValueOfSharesOffered"])
        

    # dataframe with stock info
    nasdaq_priced_df = pd.DataFrame({"symbol" : symbol_list, 
                    "company" : company_list, 
                    "offer_date" : offer_date_list,
                    "market_cap_offered" : market_cap_list
                    })

    nasdaq_priced_df["date_type"] = "Confirmed"
    nasdaq_priced_df['source'] = "Nasdaq"

    #############################                                
    # Nasdaq Upcoming IPOs
    #############################

    upcoming_ipos = data["data"]["upcoming"]["upcomingTable"]["rows"]

    symbol_list = []
    company_list = []
    offer_date_list = []
    market_cap_list = []

    for x in range(len(upcoming_ipos)):
        symbol_list.append(upcoming_ipos[x]["proposedTickerSymbol"])
        company_list.append(upcoming_ipos[x]["companyName"])
        offer_date_list.append(upcoming_ipos[x]["expectedPriceDate"])
        market_cap_list.append(upcoming_ipos[x]["dollarValueOfSharesOffered"])
        

    # dataframe with stock info
    nasdaq_upcoming_df = pd.DataFrame({"symbol" : symbol_list, 
                    "company" : company_list, 
                    "offer_date" : offer_date_list,
                    "market_cap_offered" : market_cap_list
                    })

    nasdaq_upcoming_df["date_type"] = "Expected"
    nasdaq_upcoming_df['source'] = "Nasdaq"

    #############################                                
    # Combine all IPOs
    #############################

    # combine IPO dataframes
    ipo_df = pd.concat([ipo_scoop_recent_df, ipo_scoop_upcoming_df, nasdaq_priced_df, nasdaq_upcoming_df], ignore_index=True, sort=False)

    # convert offer date to datetime datatype
    ipo_df['offer_date'] = pd.to_datetime(ipo_df['offer_date'], format="%m/%d/%Y")
    ipo_df = ipo_df.sort_values(by='date_type', ascending=True) # sort by date_type to keep "confirmed" values for duplicates if results differ

    # drop duplicate symbols, if there is a confirmed keep the first so that "expected" is dropped
    ipo_df = ipo_df.drop_duplicates(subset='symbol', keep="first")

    ##########################################                    
    # Determine New Symbols - Add to Database
    ##########################################

    ipo_df = pd.concat([ipo_scoop_recent_df, ipo_scoop_upcoming_df, nasdaq_priced_df, nasdaq_upcoming_df], ignore_index=True, sort=False)
    
    # drop unnecessary columns if they exist
    ipo_df = ipo_df.drop(['Company','Symbol proposed','Expected to Trade','Rating Change', 'Lead Managers'], axis=1, errors='ignore')
    ipo_df.dropna(subset=['symbol'])

    # convert offer date to datetime datatype
    ipo_df['offer_date'] = pd.to_datetime(ipo_df['offer_date'], format="%m/%d/%Y")
    ipo_df = ipo_df.sort_values(by='date_type', ascending=True) # sort by date_type to keep "confirmed" values for duplicates if results differ

    print("IPOs scraped!")
    return ipo_df

