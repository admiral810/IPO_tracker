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
# sys.path.insert(0, '../../Key')
# from mysql_secret import dbuser, dbpass, dbhost, dbname
# engine = create_engine(f'mysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8')

# Establish Twitter connection
import tweepy

# get tweepy credentials
sys.path.insert(0, '../../Key')
from tweepy_keys import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)

print(CONSUMER_KEY)