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

# import local modules
from scrape_ipo import scrape_for_ipos
import sql_updates as su

# get new IPOs dataframe
ipo_df = scrape_for_ipos()

# add new IPO symbols to postgres
su.new_ipos_to_sql(ipo_df)

