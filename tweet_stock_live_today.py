# import libraries
import requests
import time
from datetime import datetime, date, timedelta
import json
import sys
import random
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
from pandas.plotting import table

import seaborn as sns
colors = ["#118AB2", "#EF476F", "#FFD166", "#06D6A0", "#EE754D", "#002E99"]

import plotly.figure_factory as ff

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


random_goes_live_today = [
    "see what they're about below",
    "below are the #things they #do",
    "here's some more info",
    "#read about them below",
    "what are they about? just look below...",
    "who the heck are they? this is the heck that they are...",
    "wanna learn more?",
    "are you gonna buy them or not buy them?", 
    "might buy them or might not...",
    "this is financial advise (just kidding)",
    "some things you should know...",
    "beep bop I'm a robot this is what I know about them",
    "here's what they do",
    "check em out",
    "who cares (if you do here's more on them)",
    "write up about who they are below",
    "this is what their business does", 
    "this is what they do",
    "ok more is below if you care if you don't thats fine too", 
    "thanks for stopping by here's a lil more on them below",
    "learn more below"
    ]


random_end_of_desc = [
    "yada yada yada you get it",
    "ok look if you're still reading go to their website",
    "I think we get the point",
    "you're getting bored that's enough for now",
    "blah blahhhh blah blahhhh blah we get it #amiright",
    "well that's most of it",
    "ok goofballs that's enough for now",
    "a solid mix of interesting and not interesting!", 
    "okey doke I got it",
    "you get the picture",
    "and then there's some more stuff but I digress",
    "makes sense to me by now",
    "ok... got it",
    "neat I guess",
    "sounds unique or not unique depends who you talk to",
    "omg WE GET IT",
    "so that's what they do", 
    "enough story time on them for now",
    "ugh who cares", 
    "Robot needs a rest that's enough info for now",
    "as there's a drive into deep left field by Castellanos and that'll be a home run, and so that'll make it a 4-0 ballgame"
]

#######################################
###  UPDATE CHARACTERISTICS TABLE   ### 
#######################################
# get charactistics data for companies missing industry or sector
comp_char_update_df = idr.get_update_industry_sector()

# update database for companies missing industry or sector that now have one
su.update_industry_sector_to_sql(comp_char_update_df)
#######################################


# get todays date
today = datetime.today().strftime("%Y-%m-%d")

# get stocks that go live today
engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')
connection = engine.connect()
df = pd.read_sql(f"""
    SELECT s.*, 
        ci.city, 
        ci.state,
        ci.country,
        ci.website, 
        ci.industry,
        ci.sector, 
        ci.business_summary
    FROM stocks s
    LEFT JOIN company_info ci
        ON s.symbol = ci.symbol
    WHERE priced_date = '{today}'
        AND exchange <> 'NASDAQ Capital'""", connection)
connection.close()
df['region'] = np.where(df['country']=='United States', df['city'] + ", " + df['state'], 
                        df['city'] + ", " + df['country'])

# filter out SPACS
df = df.loc[(df["exchange"] != 'NASDAQ Capital')]
df = df.loc[~(df["company"].str.contains("Acquisition", regex=False))]  #any company that contains "Acquisition"

# keep only ones that have region and industry
df = df.dropna(subset=['region', 'industry'])

# add market cap
df["market_cap"] = (df['dollar_val_shares'].astype(float)/1000000).round(1).astype(str) + 'M'

row_count = len(df.index)

if row_count > 0:
    for row in df.itertuples():
        
        symbol = row.symbol
        company = row.company
        industry = row.industry.lower()
        region = row.region
        business_summary = "\n\n\n" + row.business_summary
        business_summary.replace('\\n', '\n')

        # shorten text some
        business_summary = business_summary[:455] + "... "
        
        # get tweet text
        tweet_text = f"{company} ${symbol} should trade today, a {industry} company out of {region} ... {random.choice(random_goes_live_today)} \n\nPrice Range: ${row.proposed_share_price} \nMarket Cap: ${row.market_cap}"
        tweet_text.replace('\\n', '\n')
        
        # text for image of company description
        description = business_summary + " " + random.choice(random_end_of_desc)
        
        # company description image
        fig = plt.figure(figsize=(6,4))
        ax = fig.add_subplot(111)
        ax.axis([0, 10, 0, 10])
        ax.text(5, 10, description, fontsize=12, style='normal', ha='center',
                 va='top', wrap=True, bbox={'facecolor': 'none', 'edgecolor': 'none', 'alpha': 0.2, 'pad': 5})
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.axes.xaxis.set_visible(False)
        ax.axes.yaxis.set_visible(False)
        plt.savefig('comp_desc.png')
        plt.show

        # the name of the media file 
        filename = "comp_desc.png"

        # upload the file 
        media = api.media_upload(filename) 
        
        # Send out the tweet
        api.update_with_media(filename="comp_desc.png", media_id=media.media_id, status=tweet_text)
        
        time.sleep(240)


su.update_sql_log("stock goes live today")