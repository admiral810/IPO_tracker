# import libraries
import requests
import time
from datetime import datetime, date, timedelta
import json
import sys
import random
import numpy as np
import pandas as pd

# matplotlib
import matplotlib.pyplot as plt
from pandas.plotting import table

# seaborn
import seaborn as sns
colors = ["#118AB2", "#EF476F", "#FFD166", "#06D6A0", "#EE754D", "#002E99"]

# plotly  https://plotly.com/python/figure-factory-table/
import plotly.figure_factory as ff

# sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

# mysql login info
sys.path.insert(0, '../../Key')
from mysql_secret import dbuser, dbpass, dbhost, dbname
engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')

# import local modules
import ipo_data_retrieval as idr
import sql_updates as su

# Establish Twitter connection
import tweepy

# get tweepy credentials
sys.path.insert(0, '../../Key')
from tweepy_keys import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)

random_upcoming_ipo = [
    "check em out I guess",
    "I dunno who cares",
    "if you care",
    "are these good I dunno",
    "whats the point",
    "are any good?",
    "worth buying?",
    "are they overvalued",
    "any kewl ones?",
    "if you wanna dip a toe into em",
    "could be good or duds", 
    "this week",
    "... neat",
    "#stonks lmao right?",
    "from my robot",
    "coming at ya",
    "we'll see I guess", 
    "just cuz",
    "per the internet",
    "beep bop Im a robot",
    "schedule accordingly",
    "pass... or don't up to you",
    "at least thats the plan", 
    "have a good week",
    "kewl kewl kewl",
    "might buy all or none or some",
    "thanks for stopping by"
    ]

def remove_comp_suffix(df, company='company'):
    """Removes traling company name info that takes up space."""
    remove_text = [' /FL', '/DE', ', Inc.', ' Inc.', ', INC.', ' INC.',' PLC', ', Corp.', 'corp.', ' Corp.', ' CORP.', 'Ltd.', 
                   ', Ltd', ' Ltd', ' plc', ' PLC']
    for t in remove_text:
        df[company] = df[company].str.replace(t, "")
    
    return df      


######################################################
# CONSTRUCT AND SEND TWEET
######################################################

# get range for today until one week from today
today = date.today()
week_from_today = today + timedelta(days=7)

#convert to string for query purposes from SQL
today = today.strftime("%Y-%m-%d")
week_from_today = week_from_today.strftime("%Y-%m-%d")

print(f"Range is {today} to {week_from_today}")

# get upcoming IPOs
connection = engine.connect()
tw_ipos_df = pd.read_sql(f"""
    SELECT *
    FROM stocks
    WHERE priced_date BETWEEN '{today}' and '{week_from_today}'
    """, connection)


# convert to datetime
tw_ipos_df['priced_date'] =  pd.to_datetime(tw_ipos_df['priced_date'])

# add column for month day
tw_ipos_df["price_month_day"] = tw_ipos_df['priced_date'].dt.strftime('%b %d')

# add market cap
tw_ipos_df["market_cap"] = (tw_ipos_df['dollar_val_shares'].astype(float)/1000000).round(1).astype(str) + 'M'

# filter out SPACS
tw_ipos_df = tw_ipos_df.loc[(tw_ipos_df["exchange"] != 'NASDAQ Capital')]
tw_ipos_df = tw_ipos_df.loc[~(tw_ipos_df["company"].str.contains("Acquisition", regex=False))]  #any company that contains "Acquisition"

tw_ipos_df = tw_ipos_df.sort_values("priced_date")

# remove inc. and corp. and stuff like that from company name
tw_ipos_df = remove_comp_suffix(tw_ipos_df)

# if there are no rows stop the function
if (len(tw_ipos_df.index) == 0):
    print("no upcoming IPOs")

else:
    ###########################
    # GET TWEET TEXT 
    ###########################
    tweet_text = f"Upcoming IPOs {random.choice(random_upcoming_ipo)}\n"

    for row in tw_ipos_df.itertuples():
        
        # remove .00 from proposed share prices to reduce unecesary characters
        psp = row.proposed_share_price.replace(".00", "")
        
        # if going to exceed the length of twitter characters allowed then break out of loop
        if len(tweet_text) > 245:
            break
        else:
            tweet_text += "\n"
            row_text = f"{row.price_month_day}: ${row.symbol} at ${psp}" 
            tweet_text += row_text


    ###########################
    # GET TWEET IMAGE 
    ###########################

    # table for tweet image
    tw_ipos_img_df = tw_ipos_df[['price_month_day', 'symbol', 'company', 'proposed_share_price', 'market_cap']]
    
    # add a blank column for text overlap
    tw_ipos_img_df[" "] = " "
    tw_ipos_img_df = tw_ipos_img_df[['price_month_day', 'symbol', 'company', ' ', 'proposed_share_price', 'market_cap']]

    tw_ipos_img_df['proposed_share_price'] = "$" + tw_ipos_img_df['proposed_share_price']

    tw_ipos_img_df = tw_ipos_img_df.rename(columns={'price_month_day': 'Date',
                                                    'symbol': 'Symbol',
                                                    'company': 'Company',
                                                    'proposed_share_price': 'Proposed Price',
                                                    'market_cap': 'Market Cap'
                                                })

    # get tweet image
    colorscale = [[0, '#118AB2'],[.5, '#FFFFFF'],[1, '#F2F2F2']]
    fig =  ff.create_table(tw_ipos_img_df, colorscale=colorscale, height_constant=20)

    fig.write_image("weekly_ipos.png", scale = 1)

    ###########################
    # SEND TWEET 
    ###########################

    # the name of the media file 
    filename = "weekly_ipos.png"

    # upload the file 
    media = api.media_upload(filename) 

    # Send out the tweet
    api.update_with_media(filename="weekly_ipos.png", media_id=media.media_id, status=tweet_text)


su.update_sql_log("upcoming IPOs schedule")