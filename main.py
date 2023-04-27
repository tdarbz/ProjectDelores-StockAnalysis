# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import sqlalchemy
import pymysql
import pandas as pd
import yfinance as yf
import numpy as np


pymysql.install_as_MySQLdb()
indices = ['SP500', 'NasdaqComposite', 'DowJonesIndustrialAverage']

'''mydb = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    passwd = 'Qazwsx1707360',
)'''

#my_cursor = mydb.cursor()
#SP500 = my_cursor.execute("CREATE DATABASE SP500")
#Nasdaq = my_cursor.execute("CREATE DATABASE NASDAQ")

tickersSP500 = ['MMM', 'AOS', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI','ADM','ADBE','ADP','AAP','AES','AFL','A','APD','AKAM'
    ,'ALK','ALB','ARE','ALGN','ALLE','LNT','ALL','GOOGL','GOOG','MO','AMZN','AMCR','AMD','AEE','AAL','AEP','AXP','AIG',
                'AMT','AWK','AMP','ABC','AME','AMGN','APH','ADI','ANSS','AON','APA','AAPL','AMAT','APTV','ANET','AJG','AIZ','T','ATO','ADSK','AZO','AVB','AVY','BKR','BALL','BAC','BBWI','BAX','BDX','WRB','BRK.B','BBY','BIO','TECH','BIIB','BLK','BK','BA','BKNG','BWA','BXP','BSX','BMY','AVGO','BR','BRO','BF.B ','CHRW','CDNS','CZR','CPT','CPB','COF','CAH','KMX','CCL','CARR','CTLT','CAT','CBOE','CBRE','CDW','CE','CNC','CNP','CDAY','CF','CRL','SCHW','CHTR','CVX','CMG','CB','CHD','CI','CINF','CTAS','CSCO','C','CFG','CTXS','CLX','CME','CMS','KO','CTSH','CL','CMCSA','CMA','CAG','COP','ED','STZ','CEG','COO','CPRT','GLW','CTVA','COST','CTRA','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','XRAY','DVN','DXCM','FANG','DLR','DFS','DISH','DIS','DG','DLTR','D','DPZ','DOV','DOW','DTE','DUK','DRE','DD','DXC','EMN','ETN','EBAY','ECL','EIX','EW','EA','ELV','LLY','EMR','ENPH','ETR','EOG','EPAM','EFX','EQIX','EQR','ESS','EL','ETSY','RE','EVRG','ES','EXC','EXPE','EXPD','EXR','XOM','FFIV','FDS','FAST','FRT','FDX','FITB','FRC','FE','FIS','FISV','FLT','FMC','F','FTNT','FTV','FBHS','FOXA','FOX','BEN','FCX','GRMN','IT','GNRC','GD','GE','GIS','GM','GPC','GILD','GL','GPN','GS','HAL','HIG','HAS','HCA','PEAK','HSIC','HSY','HES','HPE','HLT','HOLX','HD','HON','HRL','HST','HWM','HPQ','HUM','HBAN','HII','IBM','IEX','IDXX','ITW','ILMN','INCY','IR','INTC','ICE','IP','IPG','IFF','INTU','ISRG','IVZ','IQV','IRM','JBHT','JKHY','J','JNJ','JCI','JPM','JNPR','K','KDP','KEY','KEYS','KMB','KIM','KMI','KLAC','KHC','KR','LHX','LH','LRCX','LW','LVS','LDOS','LEN','LNC','LIN','LYV','LKQ','LMT','L','LOW','LUMN','LYB','MTB','MRO','MPC','MKTX','MAR','MMC','MLM','MAS','MA','MTCH','MKC','MCD','MCK','MDT','MRK','META','MET','MTD','MGM','MCHP','MU','MSFT','MAA','MRNA','MHK','MOH','TAP','MDLZ','MPWR','MNST','MCO','MS','MOS','MSI','MSCI','NDAQ','NTAP','NFLX','NWL','NEM','NWSA','NWS','NEE','NLSN','NKE','NI','NDSN','NSC','NTRS','NOC','NLOK','NCLH','NRG','NUE','NVDA','NVR','NXPI','ORLY','OXY','ODFL','OMC','ON','OKE','ORCL','OGN','OTIS','PCAR','PKG','PARA','PH','PAYX','PAYC','PYPL','PENN','PNR','PEP','PKI','PFE','PM','PSX','PNW','PXD','PNC','POOL','PPG','PPL','PFG','PG','PGR','PLD','PRU','PEG','PTC','PSA','PHM','PVH','QRVO','PWR','QCOM','DGX','RL','RJF','RTX','O','REG','REGN','RF','RSG','RMD','RHI','ROK','ROL','ROP','ROST','RCL','SPGI','CRM','SBAC','SLB','STX','SEE','SRE','NOW','SHW','SBNY','SPG','SWKS','SJM','SNA','SEDG','SO','LUV','SWK','SBUX','STT','STE','SYK','SIVB','SYF','SNPS','SYY','TMUS','TROW','TTWO','TPR','TGT','TEL','TDY','TFX','TER','TSLA','TXN','TXT','TMO','TJX','TSCO','TT','TDG','TRV','TRMB','TFC','TWTR','TYL','TSN','USB','UDR','ULTA','UNP','UAL','UPS','URI','UNH','UHS','VLO','VTR','VRSN','VRSK','VZ','VRTX','VFC','VTRS','VICI','V','VNO','VMC','WAB','WBA','WMT','WBD','WM','WAT','WEC','WFC','WELL','WST','WDC','WRK','WY','WHR','WMB','WTW','GWW','WYNN','XEL','XYL','YUM','ZBRA','ZBH','ZION','ZTS',]

tickersNAS100 = ['ATVI','ADBE','ADP','ABNB','ALGN','GOOGL','GOOG','AMZN','AMD','AEP','AMGN','ADI','ANSS','AAPL','AMAT',
                 'ASML','AZN','TEAM','ADSK','BIDU','BIIB','BKNG','AVGO','CDNS','CHTR','CTAS','CSCO','CTSH','CMCSA','CEG'
        ,'CPRT','COST','CRWD','CSX','DDOG','DXCM','DOCU','DLTR','EBAY','EA','EXC','FAST','FISV','FTNT','GILD','HON',
                 'IDXX','ILMN','INTC','INTU','ISRG','JD','KDP','KLAC','KHC','LRCX','LCID','LULU','MAR','MRVL','MTCH',
                 'MELI','META','MCHP','MU','MSFT','MRNA','MDLZ','MNST','NTES','NFLX','NVDA','NXPI','ORLY','OKTA','ODFL',
                 'PCAR','PANW','PAYX','PYPL','PEP','PDD','QCOM','REGN','ROST','SGEN','SIRI','SWKS','SPLK','SBUX','SNPS',
                 'TMUS','TSLA','TXN','VRSN','VRSK','VRTX','WBA','WDAY','XEL','ZM','ZS']



def schemacreator(index):
    engine = sqlalchemy.create_engine("mysql://root:Qazwsx1707360@localhost:3306/")
    #engine = create_engine("mysql://user:pwd@localhost/college", echo=True)
    engine.execute(sqlalchemy.schema.CreateSchema(index))

for index in indices:
    schemacreator(index)

#scanning the web for S&P tickers
spy = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
spy = spy.Symbol.to_list()
print(spy)

#scaning the web for Nasdaq tickers
nasdaq = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
nasdaq = nasdaq.Ticker.to_list()

#scanning the web for Dow Industrial tickers
dowjonesindustrial = pd.read_html("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")[1]
dowjonesindustrial = dowjonesindustrial.Symbol.to_list()

nazzy = yf.download(nasdaq[0],start='2020-01-01')
print(nazzy)

mapper = {'SP500': spy, 'NasdaqComposite': nasdaq, 'DowJonesIndustrialAverage': dowjonesindustrial}

for index in indices:
    engine = sqlalchemy.create_engine("mysql://root:Qazwsx1707360@localhost:3306/" + index)
    for symbol in mapper[index]:
        df = yf.download(symbol, start='2018-01-01')
        df = df.reset_index()
        df.to_sql(symbol, engine)

