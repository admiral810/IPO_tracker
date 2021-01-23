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

sys.path.insert(0, '../../Key')
from mysql_secret import dbuser, dbpass, dbhost, dbname


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

    # change column datatypes
    ipo_df[['shares_offered', 'dollar_val_shares']] = ipo_df[['shares_offered', 'dollar_val_shares']].apply(pd.to_numeric)
    ipo_df['priced_date'] = pd.to_datetime(ipo_df['priced_date'], format="%m/%d/%Y")
    ipo_df = ipo_df.sort_values(by='deal_status', ascending=False).reset_index(drop=True)
    ipo_df = ipo_df.drop_duplicates(subset="symbol", keep="first")
    ipo_df = ipo_df.dropna()
    return ipo_df



def scrape_for_performance():

    #######################################################                                
    # GET UNIX TIMES FOR SYMBOLS (avoids duplication)
    #######################################################

    # create engine
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')
    connection = engine.connect()

    # get unix time range for web request
    start_unixtime = 1514903400  #Jan 2, 2018

    current_date = datetime.now()
    print(current_date)

    earliest_date_time = current_date.replace(hour=16, minute=30, second=0, microsecond=0)
    print(earliest_date_time)

    end_unixtime = time.mktime(earliest_date_time.timetuple())
    end_unixtime = int(end_unixtime)

    # today
    today = datetime.today().strftime("%Y-%m-%d")

    # query stocks from SQL
    connection = engine.connect()
    ipo_stocks = pd.read_sql(f"SELECT symbol, deal_status FROM stocks WHERE deal_status = 'priced' OR priced_date = '{today}'", connection)
    ipo_stocks["default_start_unixtime"] = start_unixtime
    ipo_stocks["end_unixtime"] = end_unixtime

    # get the max unix captured to avoid double capturing the same date twice (pick up where database left off)
    stock_perf_start_unix = pd.read_sql("SELECT symbol, max(unix_time) AS max_unix_captured  FROM performance  GROUP BY symbol""", connection)
    ipo_stocks = ipo_stocks.merge(stock_perf_start_unix, on="symbol", how="outer")
    ipo_stocks["max_unix_captured"] = ipo_stocks["max_unix_captured"].fillna(0).astype('int64')
    ipo_stocks["max_unix_captured"] = ipo_stocks["max_unix_captured"] + 86400  #add a day to latest date captured

    # get the updated 'start' unix_time
    ipo_stocks["start_unixtime"] = ipo_stocks[["default_start_unixtime", "max_unix_captured"]].max(axis=1).astype('int64')

    #######################################################                                
    # GET PERFORMANCE DATA
    #######################################################

    # empty list of dfs
    performance_df_list= []

    # make requests for performance information for each row
    for row in ipo_stocks.itertuples():

        url = f'https://query2.finance.yahoo.com/v8/finance/chart/{row.symbol}?formatted=true&crumb=T18HKACbWPn&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1={row.start_unixtime}&period2={row.end_unixtime}&events=div%7Csplit&corsDomain=finance.yahoo.com'
        r = requests.get(url)
        print(f"trying url: {url}")
        if r.ok:
            try: 
                data = r.json()

                # get data
                timestamp = data["chart"]["result"][0]["timestamp"]
                stk_open = data["chart"]["result"][0]["indicators"]["quote"][0]["open"]
                stk_close = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                stk_high = data["chart"]["result"][0]["indicators"]["quote"][0]["high"]
                stk_low = data["chart"]["result"][0]["indicators"]["quote"][0]["low"]
                stk_vol = data["chart"]["result"][0]["indicators"]["quote"][0]["volume"]

                #transform into dataframe
                df = pd.DataFrame({"symbol" : row.symbol,
                                "unix_time" : timestamp,
                                "date" : [datetime.fromtimestamp(ts).strftime('%Y-%m-%d') for ts in timestamp],
                                "open" : stk_open, 
                                "close" : stk_close,
                                "high" : stk_high,
                                "low" : stk_low,
                                "volume" : stk_vol
                                })

                df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
                df['date_pulled'] = today

                print(f"{row.symbol} has results")
                performance_df_list.append(df)
            except KeyError:
                continue
        
        else:
            print(f"{row.symbol} NO RESULTS")

        time.sleep(.5)

    # combine all the performance results to one dataframe if there are dataframes with data else return None
    if len(performance_df_list) > 0:
        performance_df = pd.concat(performance_df_list)

        # first cleaning step (commented out for now):  remove unix time not divisible by 100 as all desired data appears to have unix divisable by 100
        # performance_df = performance_df.loc[performance_df["unix_time"] % 100 == 0]

        # second clearning step:  remove remaining DUPES
        performance_df = performance_df.drop_duplicates(subset=['symbol', 'date'], keep='first')

        return performance_df

    else: 
        return None



def scrape_for_ind_performance():

    #######################################################                                
    # GET UNIX TIMES FOR SYMBOLS (avoids duplication)
    #######################################################

    # create engine
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')
    connection = engine.connect()

    # get unix time range for web request
    start_unixtime = 1514903400  #Jan 2, 2018

    current_date = datetime.now()
    print(current_date)

    earliest_date_time = current_date.replace(hour=16, minute=30, second=0, microsecond=0)
    print(earliest_date_time)

    end_unixtime = time.mktime(earliest_date_time.timetuple())
    end_unixtime = int(end_unixtime)

    today = datetime.today().strftime("%Y-%m-%d")

    # industry index's list for requests
    ind_idx_lookup_list = ['%5EDJI', '%5EIXIC', '%5EGSPC']
    ind_idx_symbol_list = ['DJI', 'IXIC', 'GSPC']

    # initial industry dataframe to interate through for requests
    ind_df = pd.DataFrame({'symbol_lookup': ind_idx_lookup_list,
                        'symbol': ind_idx_symbol_list})
    ind_df["default_start_unixtime"] = start_unixtime
    ind_df["end_unixtime"] = end_unixtime

    # update to only pull new date information
    ind_perf_start_unix = pd.read_sql("SELECT symbol, max(unix_time) AS max_unix_captured  FROM industry_performance  GROUP BY symbol""", connection)
    ind_df = ind_df.merge(ind_perf_start_unix, on="symbol", how="outer")
    ind_df["max_unix_captured"] = ind_df["max_unix_captured"].fillna(0).astype('int64')
    ind_df["max_unix_captured"] = ind_df["max_unix_captured"] + 86400  #add a day to latest date captured

    ind_df["start_unixtime"] = ind_df[["default_start_unixtime", "max_unix_captured"]].max(axis=1).astype('int64')  

    #######################################################                                
    # GET PERFORMANCE DATA
    #######################################################

    # empty list of dfs
    performance_df_list= []

    # make requests for performance information for each row
    for row in ind_df.itertuples():

        url = f'https://query2.finance.yahoo.com/v8/finance/chart/{row.symbol_lookup}?formatted=true&crumb=T18HKACbWPn&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1={row.start_unixtime}&period2={row.end_unixtime}&events=div%7Csplit&corsDomain=finance.yahoo.com'
        r = requests.get(url)
        print(f"trying url: {url}")
        if r.ok:
            try: 
                data = r.json()

                # get data
                timestamp = data["chart"]["result"][0]["timestamp"]
                stk_open = data["chart"]["result"][0]["indicators"]["quote"][0]["open"]
                stk_close = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                stk_high = data["chart"]["result"][0]["indicators"]["quote"][0]["high"]
                stk_low = data["chart"]["result"][0]["indicators"]["quote"][0]["low"]
                stk_vol = data["chart"]["result"][0]["indicators"]["quote"][0]["volume"]

                #transform into dataframe
                df = pd.DataFrame({"symbol" : row.symbol,
                                "unix_time" : timestamp,
                                "date" : [datetime.fromtimestamp(ts).strftime('%Y-%m-%d') for ts in timestamp],
                                "open" : stk_open, 
                                "close" : stk_close,
                                "high" : stk_high,
                                "low" : stk_low,
                                "volume" : stk_vol
                                })

                df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
                df['date_pulled'] = today

                print(f"{row.symbol} has results")
                performance_df_list.append(df)
            except KeyError:
                continue
        
        else:
            print(f"{row.symbol} NO RESULTS")

        time.sleep(.5)

    # combine all the performance results to one dataframe if there are dataframes with data else return None
    if len(performance_df_list) > 0:
        ind_performance_df = pd.concat(performance_df_list)

        # first cleaning step (commented out for now):  remove unix time not divisible by 100 as all desired data appears to have unix divisable by 100
        # ind_performance_df = ind_performance_df.loc[ind_performance_df["unix_time"] % 100 == 0]

        # second clearning step:  remove remaining DUPES
        ind_performance_df = ind_performance_df.drop_duplicates(subset=['symbol', 'date'], keep='first')

        return ind_performance_df

    else: 
        return None


def scrape_for_comp_char():

    # create engine
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')
    connection = engine.connect()
    
    # get symbols from SQL
    symbols_df = pd.read_sql("SELECT symbol FROM stocks", connection)

    # get symbols that already have company info
    symb_in_compinfo = pd.read_sql("SELECT symbol FROM company_info", connection)

    # remove any symbols company info already avaialable for, only keeping "new" symbols
    symbols_df = symbols_df[~symbols_df["symbol"].isin(symb_in_compinfo["symbol"])]

    #get characteristics dataframe
    today = datetime.today().strftime("%Y-%m-%d")

    # empty list for df
    symbol = []
    address = []
    city = []
    state = []
    zip_code = []
    country = []
    website = []
    industry = []
    sector = []
    business_summary = []

    # make requests for characteristic information for each row
    for row in symbols_df.itertuples():

        url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{row.symbol}?modules=assetProfile%2CearningsHistory'
        r = requests.get(url)
        print(f"trying url: {url}")
        if r.ok:
            try: 
                data = r.json()
                char_dict = data["quoteSummary"]["result"][0]["assetProfile"]

                symbol.append(row.symbol)
                address.append(char_dict.get("address1", ""))
                city.append(char_dict.get("city", ""))
                state.append(char_dict.get("state", ""))  
                zip_code.append(char_dict.get("zip", ""))  
                country.append(char_dict.get("country", ""))  
                website.append(char_dict.get("website", ""))
                industry.append(char_dict.get("industry", ""))  
                sector.append(char_dict.get("sector", ""))  
                
                # shorten the character length of the full business summary
                bus_summary = char_dict.get("longBusinessSummary", "")
                if len(bus_summary) > 252:
                    bus_summary = bus_summary[:995] + '...'
                business_summary.append(bus_summary)
                    

            except KeyError:
                continue
        
        else:
            print(f"{row.symbol} NO RESULTS")

        time.sleep(.25)

    #transform into dataframe
    comp_info_df = pd.DataFrame({"symbol": symbol,
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "country": country,
                    "website": website,
                    "industry": industry,
                    "sector": sector,
                    "business_summary": business_summary})
    comp_info_df['date_pulled'] = today

    return comp_info_df


def get_update_industry_sector():
    # query characteristics with NULL or blank values for industry or sector
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')
    connection = engine.connect()
    ci_limited_df = pd.read_sql("""
                            SELECT *
                            FROM company_info
                            WHERE industry IS NULL 
                                OR industry = ''
                                OR sector IS NULL
                                OR sector = ''""", connection)
    connection.close()

    #get characteristics dataframe
    today = datetime.today().strftime("%Y-%m-%d")

    # empty list for df
    symbol = []
    address = []
    city = []
    state = []
    zip_code = []
    country = []
    website = []
    industry = []
    sector = []
    business_summary = []

    # make requests for characteristic information for each row
    for row in ci_limited_df.itertuples():

        url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{row.symbol}?modules=assetProfile%2CearningsHistory'
        r = requests.get(url)
        print(f"trying url: {url}")
        if r.ok:
            try: 
                data = r.json()
                char_dict = data["quoteSummary"]["result"][0]["assetProfile"]

                symbol.append(row.symbol)
                address.append(char_dict.get("address1", ""))
                city.append(char_dict.get("city", ""))
                state.append(char_dict.get("state", ""))  
                zip_code.append(char_dict.get("zip", ""))  
                country.append(char_dict.get("country", ""))  
                website.append(char_dict.get("website", ""))
                industry.append(char_dict.get("industry", ""))  
                sector.append(char_dict.get("sector", ""))  
                
                # shorten the character length of the full business summary
                bus_summary = char_dict.get("longBusinessSummary", "")
                if len(bus_summary) > 252:
                    bus_summary = bus_summary[:995] + '...'
                business_summary.append(bus_summary)
                    

            except KeyError:
                continue
        
        else:
            print(f"{row.symbol} NO RESULTS")

        time.sleep(.25)

    #transform into dataframe
    comp_info_df = pd.DataFrame({"symbol": symbol,
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "country": country,
                    "website": website,
                    "industry": industry,
                    "sector": sector,
                    "business_summary": business_summary})
    comp_info_df['date_pulled'] = today

    # get dataframe for company info that has industry or sector and previously didn't
    df = comp_info_df.loc[(comp_info_df['industry'] != '')  | (comp_info_df['sector'] != '')]
    return df


def fn_get_fin_metric_index(df, metric):
    """returns the index for a given metric is in the list of metrics on yahoo (cannot use a key as it is not a dictionary).
    For example, the metric parameteter may be 'trailingMarketCap'. Other options include:
    
    trailingMarketCap, trailingForwardPeRatio, trailingPeRatio, trailingEnterpriseValue, trailingPegRatio, 
    trailingEnterprisesValueRevenueRatio, trailingEnterprisesValueEBITDARatio, trailingPsRatio, trailingPbRatio
    """
    idx = df[df["metric"] == metric].index
    idx = idx[0]
    return idx

def scrape_for_market_cap():

    #######################################################                                
    # GET UNIX TIMES FOR SYMBOLS (avoids duplication)
    #######################################################

    # create engine
    engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')
    connection = engine.connect()
    
    # get unix time range for web request
    start_unixtime = 1514903400  #Jan 2, 2018

    current_date = datetime.now()
    print(current_date)

    earliest_date_time = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    print(earliest_date_time)

    end_unixtime = time.mktime(earliest_date_time.timetuple())
    end_unixtime = int(end_unixtime)

    # query stocks from SQL
    ipo_stocks = pd.read_sql("SELECT symbol, deal_status FROM stocks WHERE deal_status = 'priced'", connection)
    ipo_stocks["default_start_unixtime"] = start_unixtime
    ipo_stocks["end_unixtime"] = end_unixtime

    # update to unix to avoid capturing duplicate records
    comp_fin_start_unix = pd.read_sql("SELECT symbol, max(unix_timestamp) AS max_unix_captured  FROM market_cap  GROUP BY symbol""", connection)
    ipo_stocks = ipo_stocks.merge(comp_fin_start_unix, on="symbol", how="outer")
    ipo_stocks["max_unix_captured"] = ipo_stocks["max_unix_captured"].fillna(0).astype('int64')
    ipo_stocks["max_unix_captured"] = ipo_stocks["max_unix_captured"] + 604800  #add a week to latest date captured

    ipo_stocks["start_unixtime"] = ipo_stocks[["default_start_unixtime", "max_unix_captured"]].max(axis=1).astype('int64') 

    #######################################################                                
    # GET MARKET CAP DATA
    #######################################################

    #get market cap dataframe
    today = datetime.today().strftime("%Y-%m-%d")

    # empty list for df
    symbol_list = []
    unix_timestamp_list = []
    date_list = []
    market_cap_list = []
    market_cap_fmt_list = []

    # make requests for characteristic information for each row
    for row in ipo_stocks.itertuples():

        url = f'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{row.symbol}?lang=en-US&region=US&symbol={row.symbol}&padTimeSeries=true&type=quarterlyMarketCap%2CtrailingMarketCap%2CquarterlyEnterpriseValue%2CtrailingEnterpriseValue%2CquarterlyPeRatio%2CtrailingPeRatio%2CquarterlyForwardPeRatio%2CtrailingForwardPeRatio%2CquarterlyPegRatio%2CtrailingPegRatio%2CquarterlyPsRatio%2CtrailingPsRatio%2CquarterlyPbRatio%2CtrailingPbRatio%2CquarterlyEnterprisesValueRevenueRatio%2CtrailingEnterprisesValueRevenueRatio%2CquarterlyEnterprisesValueEBITDARatio%2CtrailingEnterprisesValueEBITDARatio&merge=false&period1={row.start_unixtime}&period2={row.end_unixtime}&corsDomain=finance.yahoo.com'
        r = requests.get(url)
        
        if r.ok:
            time.sleep(.25)
            data = r.json()
            
            payload = data["timeseries"]["result"]
            index_list = []
            metric_list = []
            count = 0

            for d in range(0, len(payload)):
                metric = payload[d]["meta"]["type"][0] 
                metric_list.append(metric) 
                index_list.append(count)
                count += 1
                
            comp_fin_df = pd.DataFrame({"metric": metric_list},
                            index=index_list)
            
            print(f"trying url: {url}")
            try: 
                for n in range(0, len(payload[fn_get_fin_metric_index(comp_fin_df, "trailingMarketCap")]["trailingMarketCap"])):
                    symbol_list.append(row.symbol)

                    # get the market cap index using the fn_get_fin_metric_index function
                    market_cap = payload[fn_get_fin_metric_index(comp_fin_df, "trailingMarketCap")]["trailingMarketCap"]

                    unix_timestamp_list.append(payload[fn_get_fin_metric_index(comp_fin_df, "trailingMarketCap")]["timestamp"][n])
                    date_list.append(market_cap[n]["asOfDate"])
                    market_cap_list.append(market_cap[n]["reportedValue"]["raw"])
                    market_cap_fmt_list.append(market_cap[n]["reportedValue"]["fmt"])

            except KeyError:
                print(f"{row.symbol} NO RESULTS")
                continue
    

    #transform into dataframe
    market_cap_df = pd.DataFrame({"symbol": symbol_list,
                                    "unix_timestamp": unix_timestamp_list,
                                    "date": date_list,
                                    "market_cap": market_cap_list,
                                    "market_cap_formatted": market_cap_fmt_list, 
                                    "date_pulled": today
                                    })

    market_cap_df["market_cap"] = market_cap_df["market_cap"].astype('int64')
    
    return market_cap_df