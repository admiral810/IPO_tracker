# import libraries
import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
import requests
import time
from datetime import datetime
import json


def new_ipos_to_sql(df):

    # Bring in ipo table
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/IPO_tracker')
    connection = engine.connect()
    sql_ipo_df = pd.read_sql("SELECT * FROM stocks", connection)

    # indentify ipo symbols not currently in sql
    new_ipos_df = df[~df["symbol"].isin(sql_ipo_df["symbol"])]

    # load data
    new_ipos_df.to_sql('stocks', con=engine, if_exists='append', index=False)

    print(f"{len(new_ipos_df['symbol'])} were added to the database!")

    connection.close()