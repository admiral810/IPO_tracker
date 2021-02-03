# import libraries
import pandas as pd 
import numpy as np
import requests
import time
from datetime import datetime
import json
from sqlalchemy import create_engine

# import local modules
import ipo_data_retrieval as idr
import sql_updates as su


#######################################
# inital setup, pulling backdata
#######################################
# year_month_dates = ['2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12', '2019-01', 
#         '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09', '2019-10', '2019-11', '2019-12', '2020-01', '2020-02', '2020-03', '2020-04', 
#         '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12']
# for year in year_month_dates:
#     ipo_df = idr.scrape_for_ipos(year)
#     su.new_ipos_to_sql(ipo_df)


#######################################
# ongoing data updates
#######################################

# UPDATE IPOS (stocks table)
def run_ipo_stocks_updates():
    """
    Gets the upcoming IPO stocks and updates the performance table.
    """
    #######################################
    ###      UPDATE STOCKS TABLE        ### 
    #######################################
    # get new IPOs dataframe
    current_year_month = datetime.today().strftime('%Y-%m')

    # get IPO data from nasdaq
    ipo_df = idr.scrape_for_ipos(current_year_month)

    # add new IPO symbols to database
    su.new_ipos_to_sql(ipo_df)
    
    # update pending ipos with 'expected' status
    su.update_pending_ipos(ipo_df)

    #######################################
    ###   UPDATE PERFORMANCE TABLE      ### 
    #######################################

    # get performance data
    performance_df = idr.scrape_for_performance()

    # add performance data to database
    su.performance_data_to_sql(performance_df)

    #######################################
    #  UPDATE INDUSTRY PERFORMANCE TABLE  #
    #######################################

    # get industry performance data
    ind_performance_df = idr.scrape_for_ind_performance()

    # add performance data to database
    su.ind_performance_data_to_sql(ind_performance_df)

    #######################################
    ###  UPDATE CHARACTERISTICS TABLE   ### 
    #######################################

    # get charactersitics data
    comp_char_df = idr.scrape_for_comp_char()

    # add company characteristics data to database if there is at least one symbol to update
    su.comp_char_data_to_sql(comp_char_df)

    # get charactistics data for companies missing industry or sector
    comp_char_update_df = idr.get_update_industry_sector()

    # update database for companies missing industry or sector that now have one
    su.update_industry_sector_to_sql(comp_char_update_df)


    #######################################
    ###     UPDATE MARKET CAP TABLE     ###
    #######################################

    # get market cap data
    market_cap_df = idr.scrape_for_market_cap()

    # add market cap data to database if there is at least one symbol to update
    su.market_cap_data_to_sql(market_cap_df)


run_ipo_stocks_updates()



