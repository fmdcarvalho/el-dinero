from pymongo import MongoClient
from datetime import datetime
import matplotlib

import pandas as pd
import json
import time

dateformat = '%Y-%m-%dT%H:%M:%S'

class Processor(object):
    def __init__(self):
        self.client = MongoClient('mongodb://user:12341234@pnotion.com:27017')
        self.data = {}
        self.cols = ['SellVol', 'BuyVol', 'High', 'Low', 'Open', 'Close', 'Offset', 'TimeStamp', 'Next']
        self.df = []

    def get_connection(self, db, collection):
        return self.client[db][collection]

    def put_reg(self, reg):
        timestamp = reg['TimeStamp']
        timestamp = timestamp[:19]
        unixTime = time.mktime(datetime.strptime(timestamp, dateformat).timetuple())
        unixTimeSlot = int(unixTime / 30)
        if unixTime not in self.data:
            self.data[unixTimeSlot] = []
        self.data[unixTimeSlot].append(reg)

    def process_data(self):
        for period in self.data.keys():
            d = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0]
            last = self.data[period][0]
            for row in self.data[period]:
                d[0] += row['Total'] if row['OrderType'] == 'SELL' else 0
                d[1] += row['Total'] if row['OrderType'] == 'BUY' else 0
                d[2] = row['Price'] if d[2] < row['Price'] else d[2]
                d[3] = row['Price'] if d[2] > row['Price'] else d[2]
                d[4] = 0
                d[5] = 0
                d[6] = 0
                d[7] = period
                last = row if datetime.strptime(row['TimeStamp'][:19], dateformat).timetuple() < datetime.strptime(
                    last['TimeStamp'][:19], dateformat).timetuple() else last
            d[5] = last['Price']
            self.df.append(d)

        for i in range(2, len(self.df) - 1):
            self.df[i][4] = self.df[i - 1][5]
            self.df[i][6] = self.df[i - 1][4] - self.df[i - 1][5]
            self.df[i - 1][8] = self.df[i][6]

    def create_df(self):
        for reg in self.get_connection('BTC-ARK', 'buys_stripped').find({}):
            self.put_reg(reg)
        for reg in self.get_connection('BTC-ARK', 'sales_stripped').find({}):
            self.put_reg(reg)
        self.process_data()
        return pd.DataFrame(data=self.df, columns=self.cols)

if __name__ == '__main__':
    proc = Processor()
    df = proc.create_df()
    # df.sort_values(by='TimeStamp')
    df['Vol'] = df['BuyVol'] - df['SellVol']
    # df['Close'] = df['Close'] * 100000000
    df['TimeStamp'].plot()
    df['Vol'].plot()
    matplotlib.pyplot.legend(loc=4)
    matplotlib.pyplot.xlabel('time')
    matplotlib.pyplot.ylabel('vol')
    matplotlib.pyplot.show()
