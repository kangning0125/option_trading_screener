#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 11:48:47 2021

@author: kangningli
"""

import dataHelpers
import scoring
import performance
import yaml
from datetime import datetime
from pandas.tseries.offsets import BDay

import pandas as pd
import numpy as np

with open(r'my_rules.yaml') as file:
    trading_rules = yaml.full_load(file)

tickers_list = trading_rules['tickers_list']
period_end = datetime.now().strftime('%Y-%m-%d')
expiry_cutoff = trading_rules['expiry_cutoff']

# Get tickers from finviz
url = (trading_rules['finviz_url'])
if len(tickers_list) == 0:
    print('[Tickers Collection]: Retrieving stock tickers from Finviz.')
    tickers_list = dataHelpers.finviz_query(url)
    print('[Tickers Collection]: Completed! '+ str(len(tickers_list))+' tickers retrieved.')

if trading_rules['manual_tickers'] != []:
    print('[Tickers Collection]: Adding manually selected tickers to the list.')
    tickers_list.extend(trading_rules['manual_tickers'])
    tickers_list = set(tickers_list)
    
# Get option chain from yahoo finance
option_chain_df = dataHelpers.get_stock_option_chains(tickers_list, expiry_cutoff = expiry_cutoff, filter_direction = 'out')

# Pre-screen the option chain
market_date = datetime.today()
previous_business_date = market_date - BDay(3)

option_chain_df_refine = option_chain_df.loc[(option_chain_df['openInterest'] > 100) \
                                      & (option_chain_df['lastTradeDate'] >= previous_business_date),:]

option_list_raw = option_chain_df_refine['contractSymbol'].tolist()

# Calculate technical indicators from yahoo finance historical data
option_ta_df = dataHelpers.get_option_tech_df(option_list_raw, period_end, trading_rules, exclude_na = False)

option_list = option_ta_df['symbol'].tolist()
# Get market data from robinhood
option_market_data_df = dataHelpers.get_robinhood_market_data(option_list)

final_df = pd.merge(option_market_data_df, option_ta_df, on=["symbol"])

final_df_w_score = scoring.calc_bscore(final_df)

print('------------------------ Screening Completed --------------------------')
print('# of Tickers imported: ' + str(len(tickers_list)))
print('# of Option Symbols collected: ' + str(option_chain_df.shape[0]))
print('# of Option Symbols after pre-screen: ' + str(option_chain_df_refine.shape[0]))
print('# of Option Symbols imported for TA: ' + str(len(option_list_raw)))
print('# of Option Symbols processed for B-Score: ' + str(final_df_w_score.shape[0]))
print('----------------------------------------------------------------------')

print('------------------- Starting Performance Tracking ---------------------')
performance.record_buy_symbols(datetime.today(), final_df_w_score, 15)
performance.calc_performance(datetime.today())
print('------------------- Performance Tracking Completed --------------------')