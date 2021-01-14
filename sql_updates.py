# import libraries
import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import requests
import time
from datetime import datetime
import json
import sys

# mysql login info
sys.path.insert(0, '../../Key')
from mysql_secret import dbuser, dbpass, dbhost, dbname

def new_ipos_to_sql(df):
    
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}')
    
    # Bring in ipo table
    connection = engine.connect()
    sql_ipo_df = pd.read_sql("SELECT * FROM stocks", connection)

    # indentify ipo symbols not currently in sql
    new_ipos_df = df[~df["symbol"].isin(sql_ipo_df["symbol"])]

    # load data
    new_ipos_df.to_sql('stocks', con=engine, if_exists='append', index=False)

    print(f"{len(new_ipos_df['symbol'])} were added to the database!")

    connection.close()



def update_pending_ipos(df):

    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}')
    connection = engine.connect()

    # Bring in ipo table
    sql_ipo_df = pd.read_sql("SELECT * FROM stocks", connection)

    # query IPOs with expected status in SQL database
    sql_expected = sql_ipo_df.query("deal_status == 'expected'")

    # expected stocks in SQL database that are in new IPO pull from nasdaq 
    stocks_to_update = df[df["symbol"].isin(sql_expected["symbol"])]

    # create engine
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')

    # Declare a Base using `automap_base()`
    Base = automap_base()

    # Use the Base class to reflect the database tables
    Base.prepare(engine, reflect=True)

    # Print all of the classes mapped to the Base
    Base.classes.keys()

    # Assign the ipo class to a variable called `IPO`
    Stocks = Base.classes.stocks

    # Create a session
    session = Session(engine)

    for row in stocks_to_update.itertuples():

        # Get the record we want to change
        stock = session.query(Stocks).filter(Stocks.symbol==row.symbol).first()

        # Change the record
        stock.proposed_share_price = stocks_to_update.loc[stocks_to_update["symbol"] == row.symbol, 'proposed_share_price'].iloc[0]
        stock.shares_offered = stocks_to_update.loc[stocks_to_update["symbol"] == row.symbol, 'shares_offered'].iloc[0]
        stock.priced_date = stocks_to_update.loc[stocks_to_update["symbol"] == row.symbol, 'priced_date'].iloc[0]
        stock.dollar_val_shares = stocks_to_update.loc[stocks_to_update["symbol"] == row.symbol, 'dollar_val_shares'].iloc[0]
        stock.deal_status = stocks_to_update.loc[stocks_to_update["symbol"] == row.symbol, 'deal_status'].iloc[0]

    # Update the database
    session.commit()
    session.close()




def performance_data_to_sql(df):

    if df is None:
        pass
    else:
        engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}')
        connection = engine.connect()
        
        # add performance to database
        df.to_sql('performance', con=engine, if_exists='append', index=False)
        connection.close()


def ind_performance_data_to_sql(df):

    if df is None:
        pass
    else:
        engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}')
        connection = engine.connect()
        
        # add industry performance to database
        connection = engine.connect()
        df.to_sql('industry_performance', con=engine, if_exists='append', index=False)
        connection.close()


def comp_char_data_to_sql(df):

    if len(df['symbol']) > 0:
        engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}')
        connection = engine.connect()
        
        # add industry performance to database
        connection = engine.connect()
        df.to_sql('company_info', con=engine, if_exists='append', index=False)
        connection.close()
        print(f"{len(df['symbol'])} new company characteristics rows added")
    else:
        print("no new company characteristics")


def market_cap_data_to_sql(df):

    if len(df['symbol']) > 0:
        engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}')
        connection = engine.connect()
        
        # add industry performance to database
        connection = engine.connect()
        df.to_sql('market_cap', con=engine, if_exists='append', index=False)
        connection.close()
        print(f"{len(df['symbol'])} new market cap rows added")
    else:
        print("no new market cap data")
    