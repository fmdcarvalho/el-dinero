import pandas as pd
import quandl
import math
import numpy as np
from sklearn import preprocessing, svm
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from process_data import Processor


# df = quandl.get('WIKI/GOOGL')
processor = Processor()
df = processor.create_df()
# print(df)
# df = df[['Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume']]
# print(df)

# df['HL_PCT'] = (df['Adj. High'] - df['Adj. Close']) / df['Adj. Close'] * 100
# df['PCT_change'] = (df['Adj. Close'] - df['Adj. Open']) / df['Adj. Open'] * 100
# df = df[['Adj. Close', 'HL_PCT', 'PCT_change', 'Adj. Volume']]

forecast_col = 'Offset'
df.fillna(-99999, inplace=True)
forecast_out = int(math.ceil(0.01 * len(df)))

df['label'] = df[forecast_col].shift(-forecast_out)
df.dropna(inplace=True)
df['Vol'] = df['BuyVol'] - df['SellVol']


X = np.array(df.drop(['SellVol', 'BuyVol', 'High', 'Low', 'Open', 'Close', 'Offset', 'TimeStamp', 'Next', 'label'], 1))
y = np.array(df['label'])
X = preprocessing.scale(X)
print(X)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

clf = LinearRegression()
clf.fit(X_train, y_train)
accuracy = clf.score(X_test, y_test)

print(accuracy)
