import sqlalchemy
import pymysql
import ta
import pandas as pd
import numpy as np

pymysql.install_as_MySQLdb()

engine = sqlalchemy.create_engine('mysql://root:Qazwsx1707360@localhost:3306')

def gettables(index):
    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = '{index}'"""
    df = pd.read_sql(query, engine)
    df['Schema'] = index
    return df

sp500 = gettables('SP500')
#print(sp500)
-
def getprices(which):
    prices = []
    #loop through table& schema to grab prices
    for table, schema in zip(which.TABLE_NAME, which.Schema):
        sql = schema+'.'+f'`{table}`'
        #grabbing price data
        prices.append(pd.read_sql(f"SELECT Date, Close FROM {sql}", engine))
    return prices

def MACD(df):
    df['MACD_diff'] = ta.trend.macd_diff(df.Close)
    df['Buy MACD'] = np.where((df.MACD_diff > 0) & (df.MACD_diff.shift(1) < 0), True, False)

def Bull(df):
    df['SMA9'] = ta.trend.sma_indicator(df.Close, window=9)
    df['SMA18'] = ta.trend.sma_indicator(df.Close, window=18)
    df['Signal'] = np.where(df['SMA9'] > df['SMA18'], True, False)
    df['Bullish'] = df.Signal.diff()

def Bear(df):
    df['SMA9'] = ta.trend.sma_indicator(df.Close, window=9)
    df['SMA18'] = ta.trend.sma_indicator(df.Close, window=18)
    df['Signal'] = np.where(df['SMA9'] < df['SMA18'], True, False)
    df['Bearish'] = df.Signal.diff()

def RSI(df):
    df['RSI'] = ta.momentum.rsi(df.Close, window=14)
    df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
    df['Buy RSI'] = np.where((df.Close > df.SMA200) & (df.RSI < 30), True, False)

def applyTechnicals(which):
    prices = getprices(which)
    for frame in prices:
        MACD(frame)
        RSI(frame)
        Bull(frame)
    return prices

#applyTechnicals(sp500)
getprices(sp500)
print(applyTechnicals(sp500)[0])

def callentry(which):
    indicators = ['Buy MACD','Buy RSI','Bullish']
    for symbol, frame in zip(which.TABLE_NAME, applyTechnicals(which)):
        if frame.empty is False:
            for indicator in indicators:
                if frame[indicator].iloc[-1] == True:
                    print(f"{indicator} Buying Signal for " + symbol)

class Callentry:
    engine = sqlalchemy.create_engine('mysql://root:Qazwsx1707360@localhost:3306')

    def __init__(self, index):
        self.index = index

    def gettables(self):
        query = f"""SELECT table_name FROM information_schema.tables
        Where table_schema = '{self.index}'"""
        df = pd.read_sql(query, self.engine)
        df['Schema'] = self.index
        return df

    def getprices(self):
        prices = []
        # loop through table& schema to grab prices
        for table, schema in zip(self.gettables(), self.gettables().Schema):
            sql = schema + '.' + f'`{table}`'
            # grabbing price data
            prices.append(pd.read_sql(f"SELECT Date, Close FROM {sql}", self.engine))
        return prices

    def MACD(self, df):
        df['MACD_diff'] = ta.trend.macd_diff(df.Close)
        df['Buy MACD'] = np.where((df.MACD_diff > 0) & (df.MACD_diff.shift(1) < 0), True, False)

    def RSI(self, df):
        df['RSI'] = ta.momentum.rsi(df.Close, window=14)
        df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
        df['Buy RSI'] = np.where((df.Close > df.SMA200) & (df.RSI < 30), True, False)

    def Bull(self, df):
        df['SMA9'] = ta.trend.sma_indicator(df.Close, window=9)
        df['SMA18'] = ta.trend.sma_indicator(df.Close, window=18)
        df['Signal'] = np.where(df['SMA9'] > df['SMA18'], True, False)
        df['Bullish'] = df.Signal.diff()

    def Bear(self, df):
        df['SMA9'] = ta.trend.sma_indicator(df.Close, window=9)
        df['SMA18'] = ta.trend.sma_indicator(df.Close, window=18)
        df['Signal'] = np.where(df['SMA9'] < df['SMA18'], True, False)
        df['Bearish'] = df.Signal.diff()

    def applyTechnicals(self):
        prices = self.getprices()
        for frame in prices:
            self.MACD(frame)
            self.RSI(frame)
            self.Bull(frame)
        return prices

    def callentry(self):
        indicators = ['Buy MACD', 'Buy RSI','Bullish']
        for symbol, frame in zip(self.gettables().TABLE_NAME, self.applyTechnicals()):
            if frame.empty is False:
                for indicator in indicators:
                    if frame[indicator].iloc[-1] == True:
                        print(f"{indicator} Buying Signal for " + symbol)

#SP500', 'NasdaqComposite', 'DowJonesIndustrialAverage'
#SP500instance = Callentry('sp500')
#SP500instance.callentry()
#Nasdaqinstance = Callentry('NasdaqComposite')
#Dowinstance = Callentry('DowJonesIndustrialAverage')

print(callentry(gettables('sp500')))
print(applyTechnicals(sp500)[3])