#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 11:48:55 2021

@author: kangningli
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from pandas.tseries.offsets import BDay
import requests

def record_buy_symbols(run_date, final_df_w_score, i):
    '''Record filled price of the top 5 symbols based on B-score in a dataframe.'''
    symbol_list = final_df_w_score['symbol'].tolist()[0:i]
    df_columns = ['Date']
    for symbol in symbol_list:
        df_columns.append(symbol)
        df_columns.append('return_'+symbol)
    tracking_df = pd.DataFrame(columns=df_columns)
    tracking_df.loc[0,'Date'] = run_date.strftime('%Y-%m-%d %A')
    for symbol in symbol_list:
        tracking_df.loc[0, symbol] = final_df_w_score.loc[final_df_w_score['symbol'] == symbol,'filled price'].item()
    file_date = run_date.strftime('%Y%m%d')
    tracking_df.to_csv(f'data/test_symbols_{file_date}.csv', index = False)
    print('Performance tracking file is created: '+f'data/test_symbols_{file_date}.csv')
    return tracking_df

def get_option_price_history_df(option_symbol, period_end):
    
    period_end_date = datetime.strptime(period_end, "%Y-%m-%d")
    period_start_date = period_end_date - timedelta(days = 10)
    period_start_unix = int(time.mktime(period_start_date.timetuple()))
    period_end_unix = int(time.mktime(period_end_date.timetuple()))
    
    print(f'[{option_symbol}]: [Performance Tracking] Collecting price hisotry.')
    history_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{option_symbol}?symbol={option_symbol}&period1={period_start_unix}&period2={period_end_unix}&useYfid=true&interval=1d&includePrePost=false&lang=en-US&region=US'
    option_pricing_history = requests.get(history_url).json()
    individual_pricing_history_df_columns = ['date','close','open','low','volume','high']
    option_pricing_history_df = pd.DataFrame(columns = individual_pricing_history_df_columns)

    for i, date_unix in enumerate(option_pricing_history['chart']['result'][0]['timestamp']):
        option_pricing_history_df = \
            option_pricing_history_df.append(
                                    pd.Series([
                                        datetime.fromtimestamp(date_unix).strftime('%Y-%m-%d %A'),
                                        option_pricing_history['chart']['result'][0]['indicators']['quote'][0]['close'][i],
                                        option_pricing_history['chart']['result'][0]['indicators']['quote'][0]['open'][i],
                                        option_pricing_history['chart']['result'][0]['indicators']['quote'][0]['low'][i],
                                        option_pricing_history['chart']['result'][0]['indicators']['quote'][0]['volume'][i],
                                        option_pricing_history['chart']['result'][0]['indicators']['quote'][0]['high'][i],
                                        ], index=individual_pricing_history_df_columns
                                    ), ignore_index=True
                                )
    option_pricing_history_df = option_pricing_history_df[~option_pricing_history_df.date.str.contains('Saturday|Sunday')]

    print(f'[{option_symbol}]: [Performance Tracking] Retrived price hisotry.')       
        
    return option_pricing_history_df

def calc_performance(run_date):
    '''Append past 3 days historical prices and calculate returns
    run_date: datetime
    '''
    # Search record file minus BD 4
    record_date = run_date - BDay(4)
    file_date = record_date.strftime('%Y%m%d')
    try:
        tracking_df = pd.read_csv(f'data/test_symbols_{file_date}.csv')

        # Retrieve price history
        column_list = tracking_df.columns.tolist()
        symbol_list = []
        for name in column_list:
            if 'return' not in name and 'Date' not in name:
                symbol_list.append(name)

        # Add dates and high price
        i = 1
        while i <=3:
            next_business_date = record_date + BDay(i)
            tracking_df.loc[i,'Date'] = next_business_date.strftime('%Y-%m-%d %A')
            i += 1

        for symbol in symbol_list:
            price_df = get_option_price_history_df(symbol, run_date.strftime('%Y-%m-%d'))
            for date in tracking_df['Date']:
                if date == record_date.strftime('%Y-%m-%d %A'):
                    tracking_df.loc[tracking_df['Date'] == date, 'return_'+symbol] = 0
                    continue
                else:
                    print(f'[{symbol}]: [Performance Tracking] Added price history on {date}.')
                    tracking_df.loc[tracking_df['Date'] == date, symbol] = price_df.loc[price_df['date'] == date, 'high'].item()
                    tracking_df.loc[tracking_df['Date'] == date, 'return_'+symbol] = tracking_df.loc[tracking_df['Date'] == date, symbol].item()\
                                                                                    /tracking_df.loc[tracking_df['Date'] == record_date.strftime('%Y-%m-%d %A'), symbol].item()\
                                                                                    -1
                    print(f'[{symbol}]: [Performance Tracking] Added return on {date}.')
        tracking_df.to_csv(f'data/test_symbols_{file_date}.csv')
        print('Performance tracking is completed and saved to ' + f'data/test_symbols_{file_date}.csv')
    except:
        print('Performance tracking file, '+f'test_symbols_{file_date}, '+'doesn\'t exist. Skipped the process.')