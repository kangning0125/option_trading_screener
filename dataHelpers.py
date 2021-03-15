#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 11:48:51 2021

@author: kangningli
"""

from datetime import datetime, timedelta
import time
from pandas.tseries.offsets import BDay

import numpy as np
import pandas as pd
import requests
import math
import yaml
import re

from urllib.request import Request, urlopen
from bs4 import BeautifulSoup

import yfinance as yf
import ta

import robin_stocks as rh
from getpass import getpass

with open(r'my_rules.yaml') as file:
    trading_rules = yaml.full_load(file)


def finviz_query(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    soup = BeautifulSoup(webpage, "html.parser")

    ticker_counts = soup.findAll("td", {"class": "count-text"})
    count_string = ticker_counts[0].contents[1]
    ticker_count = [int(s) for s in count_string.split() if s.isdigit()][0]
    ticker_list = []
    if ticker_count > 20:
        loop_times = int(ticker_count / 20) + (ticker_count % 20 > 0)
        loop = 1
        row_number = 1
        while loop <= loop_times:
            url_update = url + f'&r={row_number}'        
            req = Request(url_update, headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(req).read()
            soup2 = BeautifulSoup(webpage, "html.parser")        
            ticker_raw = soup2.findAll("a", {"class": "screener-link-primary"})
            for a in ticker_raw:
                ticker_list.append(a.contents[0])
            loop += 1
            row_number += 20
    else:
        ticker_raw = soup.findAll("a", {"class": "screener-link-primary"})
        for a in ticker_raw:
            ticker_list.append(a.contents[0])
    return ticker_list


def get_stock_option_chains(tickers, expiry_cutoff = None, filter_direction = 'out'):
    '''
    expiry_cutoff: 
        str, in format of 'YYYY-MM-DD';
    filter_direction: 
        str, 'in' or 'out'
        default is 'out', keeps expire dates after expire_in date.
    '''
    all_chains_df = pd.DataFrame([])
    
    for ticker in tickers:
        print(f'[Option Chain]: [{ticker}] Collecting option chain.')
        yf_ticker = yf.Ticker(ticker)
        try:
            expiry_date_list = yf_ticker.options
            if expiry_cutoff != None:
                expiry_cutoff_date = datetime.strptime(expiry_cutoff, "%Y-%m-%d")
                i = 0
                for expiry in expiry_date_list:
                    date = datetime.strptime(expiry, "%Y-%m-%d")
                    if expiry_cutoff_date > date:
                        i += 1

                if filter_direction == 'out':
                    expiry_dates = expiry_date_list[i:]
                elif filter_direction == 'in':
                    expiry_dates = expiry_date_list[0:i]
            else:
                expiry_dates = expiry_date_list  
            data = pd.DataFrame([])
            for date in expiry_dates:
                option_chain = yf_ticker.option_chain(date)
                option_chain_call_df = option_chain.calls
                option_chain_call_df['expiryDate'] = date
                data = data.append(option_chain_call_df, ignore_index=True)
            option_number = data.shape[0]
            print(f'[Option Chain]: [{ticker}] {option_number} option symbols collected.')
            data['stockSymbol'] = ticker
            all_chains_df = all_chains_df.append(data, ignore_index=True)
        except:
            print(f'[Option Chain]: [Error] [{ticker}] option info not available on Yahoo finance.')

    return all_chains_df


def get_option_tech_df(option_list, period_end, trading_rules, exclude_na = True):
    ta_columns = ['tech_last_date', 'symbol','SMA','BB - high', 'BB - low', 'RSI', 'VWAP', 'ATR', 'Range']
    option_ta_df = pd.DataFrame(columns = ta_columns)
    
    period_end_date = datetime.strptime(period_end, "%Y-%m-%d")
    period_start_date = period_end_date - timedelta(days = 40)
    period_start_unix = int(time.mktime(period_start_date.timetuple()))
    period_end_unix = int(time.mktime(period_end_date.timetuple()))
    
    sma_window = trading_rules['ta_parameters']['sma']['window']
    sma_price = trading_rules['ta_parameters']['sma']['price']
    bb_window = trading_rules['ta_parameters']['bb']['window']
    bb_price = trading_rules['ta_parameters']['bb']['price']
    bb_sigma = trading_rules['ta_parameters']['bb']['sd']
    rsi_price = trading_rules['ta_parameters']['rsi']['price']
    rsi_window = trading_rules['ta_parameters']['rsi']['window']
    vwap_window = trading_rules['ta_parameters']['vwap']['window']
    atr_window = trading_rules['ta_parameters']['atr']['window']
    pr_window = trading_rules['ta_parameters']['price_range']['window']
    
    for option_symbol in option_list:
        print(f'[{option_symbol}]: [Technical Analysis] Start processing.')
        history_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{option_symbol}?symbol={option_symbol}&period1={period_start_unix}&period2={period_end_unix}&useYfid=true&interval=1d&includePrePost=false&lang=en-US&region=US'
        try:
            option_pricing_history = requests.get(history_url).json()
        except:
            continue
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
        if option_pricing_history_df.dropna().shape[0] < 21:
            print(f'[{option_symbol}]: [Technical Analysis] Valid points are less than 21. SKIP. ')
            continue
        elif exclude_na == True:
            none_number = option_pricing_history_df['volume'].tolist().count(None)
            if none_number > 0:
                print(f'[{option_symbol}]: [Technical Analysis] Data contain {none_number} None values. SKIP.')
                continue
        elif exclude_na == False:
            option_pricing_history_df.dropna(inplace = True)

        print(f'[{option_symbol}]: [Technical Analysis] Calculating technical indicators.')
        # Calculate SMA        
        sma = ta.trend.SMAIndicator(option_pricing_history_df[sma_price], sma_window, fillna = False)
        latest_sma = sma.sma_indicator().iloc[-1]
        
        # Calculate BB - high and low bands
        bb = ta.volatility.BollingerBands(option_pricing_history_df[bb_price], bb_window, bb_sigma, fillna = False)
        latest_bb_high_band = bb.bollinger_hband().iloc[-1]
        latest_bb_low_band = bb.bollinger_lband().iloc[-1]
        
        # Calculate RSI       
        rsi = ta.momentum.rsi(option_pricing_history_df[rsi_price], rsi_window, fillna = False)
        latest_rsi = rsi.iloc[-1]
        
        # Calculate vwap        
        vwap = ta.volume.VolumeWeightedAveragePrice(option_pricing_history_df['high'],
                                                    option_pricing_history_df['low'],
                                                    option_pricing_history_df['close'],
                                                    option_pricing_history_df['volume'],
                                                    vwap_window, fillna = False)
        latest_vwap = vwap.volume_weighted_average_price().iloc[-1]
        
        # Calculate average true range
        atr = ta.volatility.AverageTrueRange(option_pricing_history_df['high'],
                                             option_pricing_history_df['low'],
                                             option_pricing_history_df['close'],
                                             atr_window, fillna = False)
        latest_atr = atr.average_true_range().iloc[-1]
        
        # Calculate price range
        price_range = option_pricing_history_df['high'].iloc[-pr_window:].max()\
                    - option_pricing_history_df['low'].iloc[-pr_window:].min()
        
        option_ta_df = option_ta_df.append(
                    pd.Series([
                        option_pricing_history_df['date'].iloc[-1],
                        option_symbol,
                        latest_sma,
                        latest_bb_high_band,
                        latest_bb_low_band,
                        latest_rsi,
                        latest_vwap,
                        latest_atr,
                        price_range], index = ta_columns
                    ), ignore_index=True
                )  
        print(f'[{option_symbol}]: [Technical Analysis] Added to option_ta_df.')       
        
    return option_ta_df


def get_robinhood_market_data(symbol_list):
    username = input('Robinhood username(Email): ')
    password = getpass('Your robinhood password: ')
    login = rh.login(username,password, expiresIn = 3600, store_session=False)
    
    market_data_df_columns = ['market date','symbol','ticker','expiry','strike','ask price','ask size','filled price','filled size',
                             'bid price','bid size','volume','OI','IV','previous close price','previous close date','today gain']
    option_market_data_df = pd.DataFrame(columns = market_data_df_columns)
    market_date = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    for option_symbol in symbol_list:
        print(f'[{option_symbol}]: [Market Data] Retriving market data from robinhood.')
        symbol_split = re.split('(\d+)',option_symbol)
        ticker = symbol_split[0]
        expiry = datetime.strptime(symbol_split[1], "%y%m%d")
        expiry_str = expiry.strftime("%Y-%m-%d")
        option_type = 'call' if symbol_split[2] == 'C' else 'put'
        strike_price = float(symbol_split[3])/1000

        option_market_data = rh.options.get_option_market_data(ticker, expiry_str, strike_price, option_type, info=None)
        option_market_data_df = option_market_data_df.append(
                                    pd.Series([
                                        market_date,
                                        option_symbol,
                                        ticker,
                                        expiry,
                                        strike_price,
                                        option_market_data[0][0]['ask_price'],
                                        option_market_data[0][0]['ask_size'],
                                        option_market_data[0][0]['last_trade_price'],
                                        option_market_data[0][0]['last_trade_size'],
                                        option_market_data[0][0]['bid_price'],
                                        option_market_data[0][0]['bid_size'],
                                        option_market_data[0][0]['volume'],
                                        option_market_data[0][0]['open_interest'],
                                        option_market_data[0][0]['implied_volatility'],
                                        option_market_data[0][0]['previous_close_price'],
                                        option_market_data[0][0]['previous_close_date'],
                                        float(option_market_data[0][0]['last_trade_price']) - float(option_market_data[0][0]['previous_close_price'])
                                        ], index=market_data_df_columns
                                    ), ignore_index=True
                                )
        print(f'[{option_symbol}]: [Market Data] Market data are added to option_market_data_df.')
    option_market_data_df.fillna(0,inplace=True)
    #option_market_data_df['today gain'] = option_market_data_df['filled price'] - option_market_data_df['previous close price']
    login = rh.authentication.logout()
    print(f'Robinhood process completed! {username} is logged out.')
    return option_market_data_df

