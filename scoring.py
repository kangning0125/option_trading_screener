#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 11:48:55 2021

@author: kangningli
"""

import pandas as pd
import numpy as np
from datetime import datetime

def calc_bscore(final_df):
    final_df['B-Score'] = 0
    for index, row in final_df.iterrows():
        b_score = 0
        #Rule 1: RSI less than 40
        if row['RSI'] < 40:
            b_score += 1
        #Rule 2: volume >= 100
        if row['volume'] >= 100:
            b_score += 1
        #Rule 3: Filled price <= lower bb
        if float(row['filled price']) <= float(row['BB - low']):
            b_score += 1
        #Rule 4: SMA 5 day <= VWAP
        if row['SMA'] <= row['VWAP']:
            b_score += 1
        #Rule 5: Range >= 0.1
        if row['Range'] >= 0.1:
            b_score += 1
        #Rule 6: Filled price = current bid
        if float(row['filled price']) == float(row['bid price']):
            b_score += 1
        #Rule 7: IV <= 40%
        if float(row['IV']) <= 0.4:
            b_score += 1
        #Rule 8: Today gain <= 0
        if row['today gain'] <= 0:
            b_score += 1
        final_df.loc[final_df.index == index,'B-Score'] = b_score
    print('B-scores are assigned to all the options.')
    final_df.sort_values(by='B-Score', ascending=False, inplace = True)
    run_date = datetime.now().strftime("%Y%m%d")
    final_df.to_csv(f'data/raw_output_{run_date}.csv', index=False)
    print('CSV data file is exported.')
    return final_df