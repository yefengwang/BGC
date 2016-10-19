# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 20:04:43 2016

@author: Yefeng Wang
"""

import pandas as pd
import operator
import datetime

MISSING = -1
NEGATIVE = 0
POSITIVE = 1
TEST = 2
UNKNOWN = 3

def generate_dates(start_date, end_date):
    '''
    Generate all dates between start_date and end_date
    '''
    dates = {}
    start = datetime.datetime.strptime(start_date, '%Y/%m/%d')
    end = datetime.datetime.strptime(end_date, '%Y/%m/%d')
    step = datetime.timedelta(days=1)
    while start <= end:
        date = start.date().strftime("%Y/%m/%d")
        date = date.replace("/0", "/")
        dates[date] = len(dates)
        start += step
    return dates

def preprocess():

    # Generate all the dates in year 2015    
    all_dates = generate_dates('2015/1/1', '2015/12/31')
    
    df_all = pd.read_csv('../data/data_2015.csv')
    
    df_train = pd.read_csv('../data/train.csv')
    df_test = pd.read_csv('../data/test.csv')
    
    # subset selection, for test purpose.
    # df_all = df_all[:5500]
    
    df_all = df_all[['CONS_NO', 'DATA_DATE', 'KWH']]

    train_data = {}
    test_data = {}
    
    for _, row in df_train.iterrows():
        cons_no, label = row[0], row[1]
        train_data[cons_no] = label    
    
    for _, row in df_test.iterrows():
        cons_no = row[0]
        test_data[cons_no] = TEST    
    
    data = []
    
    # group data by CONS_NO
    grouped = df_all.groupby(['CONS_NO'])
    
    # total number of day    
    ncol = len(all_dates)
    
    for cons_id, (cons_no, group) in enumerate(grouped):
    
        if cons_id % 100 == 0:
            print cons_id
        
        # The row contains duplicated dates        
        bad_row = 0
        
        # Stores all dates for this data
        date_indexes = set()

        # [0] is the cons_no, -1 is the label, and -2 is the bad_row indicator        
        cons = [MISSING] * (ncol + 3)
        
        cons[0] = cons_no
        for _, row in group.iterrows():
            # get the the data            
            data_date, kwh = row[1], row[2]
            # check if the date are duplicated.            
            date_index = all_dates[data_date]
            if not date_index in date_indexes:
                date_indexes.add(date_index)
            else:
                bad_row = 1
            # insert the data into corresponding column
            cons[all_dates[data_date] + 1] = kwh
        
        # Fetch the label for this point        
        label = UNKNOWN
        if train_data.has_key(cons_no):
            label = train_data[cons_no]
        elif test_data.has_key(cons_no):
            label = test_data[cons_no]
        
        cons[-1] = label
        cons[-2] = bad_row
        data.append(cons)
    
    # Header for dates    
    dates_cols = [k for k, _ in sorted(all_dates.items(), key=operator.itemgetter(1))]
    
    date_df = pd.DataFrame(data, columns=['CONS_NO'] + dates_cols + ['BAD_ROW', 'LABEL'])
    date_df.to_csv("../data/processed_dates.csv", sep=',')

if __name__ == "__main__":
    preprocess()
