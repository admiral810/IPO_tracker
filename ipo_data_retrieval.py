# import libraries
import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
import requests
import time
from datetime import datetime
import json


def scrape_for_ipos(year_month):



    #############################                                
    # Nasdaq Setup
    #############################
    
    # scrape nasdaq https://api.nasdaq.com/api/ipo/calendar?date=2020-08
    # note, had to create headers due to time out, solution found here: https://stackoverflow.com/questions/46862719/pythons-requests-library-timing-out-but-getting-the-response-from-the-browser
    url = f'https://api.nasdaq.com/api/ipo/calendar?date={year_month}'
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15","Accept-Language": "en-gb","Accept-Encoding":"br, gzip, deflate","Accept":"test/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Referer":"http://www.google.com/"}
    print(f"URL used: {url}")

    response = requests.get(url, headers=headers)
    data = response.text
    data = json.loads(data)

    # create list of scraped dataframes to concatenate (later combines priced and proposed IPOs)
    scraped_ipo_dfs = []

    #############################                                
    # Nasdaq Priced IPOs
    #############################

    # gets priced IPOs for each record on nasdaq (values) = [(expression) for (value) in (collection)]
    priced_ipos = data["data"]["priced"]["rows"]

    # if there are priced IPOs create dataframe
    if priced_ipos:
        
        symbols = [priced_ipos[x]["proposedTickerSymbol"] for x in range(len(priced_ipos))]
        company = [priced_ipos[x]["companyName"] for x in range(len(priced_ipos))]
        exchange = [priced_ipos[x]["proposedExchange"] for x in range(len(priced_ipos))]
        proposed_share_price = [priced_ipos[x]["proposedSharePrice"] for x in range(len(priced_ipos))]
        shares_offered = [priced_ipos[x]["sharesOffered"].replace(",", '') for x in range(len(priced_ipos))]
        priced_date = [priced_ipos[x]["pricedDate"] for x in range(len(priced_ipos))]
        dollar_val_shares = [priced_ipos[x]["dollarValueOfSharesOffered"].replace(",", '').replace("$",'') for x in range(len(priced_ipos))]

        # dataframe with stock info
        nasdaq_priced_df = pd.DataFrame({"symbol" : symbols,
                                        "company" : company,
                                        "exchange" : exchange, 
                                        "proposed_share_price" : proposed_share_price,
                                        "shares_offered" : shares_offered,
                                        "priced_date" : priced_date,
                                        "dollar_val_shares" : dollar_val_shares,
                                        "deal_status" : "priced"
                                        })
        scraped_ipo_dfs.append(nasdaq_priced_df)
        print(f"{len(priced_ipos)} priced IPOs")
        
    else:
        print("0 priced IPOs")

    #############################                                
    # Nasdaq Upcoming IPOs
    #############################

    upcoming_ipos = data["data"]["upcoming"]["upcomingTable"]["rows"]

    if upcoming_ipos:
        symbols = [upcoming_ipos[x]["proposedTickerSymbol"] for x in range(len(upcoming_ipos))]
        company = [upcoming_ipos[x]["companyName"] for x in range(len(upcoming_ipos))]
        exchange = [upcoming_ipos[x]["proposedExchange"] for x in range(len(upcoming_ipos))]
        proposed_share_price = [upcoming_ipos[x]["proposedSharePrice"] for x in range(len(upcoming_ipos))]
        shares_offered = [upcoming_ipos[x]["sharesOffered"].replace(",", '') for x in range(len(upcoming_ipos))]
        priced_date = [upcoming_ipos[x]["expectedPriceDate"] for x in range(len(upcoming_ipos))]
        dollar_val_shares = [upcoming_ipos[x]["dollarValueOfSharesOffered"].replace(",", '').replace("$",'') for x in range(len(upcoming_ipos))]

        # dataframe with stock info
        nasdaq_upcoming_df = pd.DataFrame({"symbol" : symbols,
                                        "company" : company,
                                        "exchange" : exchange, 
                                        "proposed_share_price" : proposed_share_price,
                                        "shares_offered" : shares_offered,
                                        "priced_date" : priced_date,
                                        "dollar_val_shares" : dollar_val_shares,
                                        "deal_status" : "expected"
                                        })
        scraped_ipo_dfs.append(nasdaq_upcoming_df)
        print(f"{len(upcoming_ipos)} upcoming IPOs")
    else: 
        print("0 upcoming IPOs")

    #############################                                
    # Combine all IPOs
    #############################

    # combine IPO dataframes
    ipo_df = pd.concat(scraped_ipo_dfs, ignore_index=True, sort=False)

    ##########################################                    
    # Determine New Symbols - Add to Database
    ##########################################

    # combine IPO dataframes
    ipo_df = pd.concat(scraped_ipo_dfs, ignore_index=True, sort=False)

    # change column datatypes
    ipo_df[['shares_offered', 'dollar_val_shares']] = ipo_df[['shares_offered', 'dollar_val_shares']].apply(pd.to_numeric)
    ipo_df['priced_date'] = pd.to_datetime(ipo_df['priced_date'], format="%m/%d/%Y")
    ipo_df = ipo_df.sort_values(by='deal_status', ascending=False).reset_index(drop=True)
    ipo_df = ipo_df.drop_duplicates(subset="symbol", keep="first")
    ipo_df = ipo_df.dropna()
    return ipo_df



def scrape_for_performance(year_month):

    # will need to build this out for ongoing
    today = datetime.today().strftime("%Y-%m-%d")