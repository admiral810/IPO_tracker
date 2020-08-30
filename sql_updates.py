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


def new_ipos_to_sql(df):

    # drop duplicate symbols, if there is a confirmed keep the first so that "expected" is dropped
    df = df.drop_duplicates(subset='symbol', keep="first")

    # trim to most relevant columns, drop any rows with NA results after trimming
    df = df[["symbol", "company", "offer_date", "date_type"]]
    df = df.dropna()

    # bring in ipo table from sql
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/IPO_tracker')
    connection = engine.connect()
    sql_ipo_df = pd.read_sql("SELECT * FROM ipo", connection)

    # indentify ipo symbols not currently in sql
    new_ipos_df = df[~df["symbol"].isin(sql_ipo_df["symbol"])]

    # load data
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/IPO_tracker')
    new_ipos_df.to_sql('ipo', con=engine, if_exists='append', index=False)

    print(f"{len(new_ipos_df['symbol'])} were added to the database!")