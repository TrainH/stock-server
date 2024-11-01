# %%
import pandas as pd
import pymysql, time
from datetime import datetime
from pykrx import stock



# %%
host='localhost'
user='root'
password='qja!$9rywns'
db='stock'
charset='utf8'

class DBUpdater_OHLCV:
    
    def __init__(self):
        # 각 프로세스에서 DB 연결을 새로 생성
        self.conn = pymysql.connect(host=host,
                                    user=user,
                                    password=password,
                                    db=db,
                                    charset=charset)
        
        self.table = 'market_ohlcv'
        
        with self.conn.cursor() as curs:
            sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        code VARCHAR(20),
                        date DATE,
                        open BIGINT(20),
                        high BIGINT(20),
                        low BIGINT(20),
                        close BIGINT(20),
                        volume BIGINT(20),
                        value BIGINT(20),
                        pct_chg FLOAT,
                        PRIMARY KEY (code, date)
                    )
                  """
            curs.execute(sql)
            
            self.conn.commit()

    def __del__(self):
        self.conn.close()
    
    def get_start_date(self):
        with self.conn.cursor() as curs:
            sql = f"SELECT max(date) FROM {self.table}"
            curs.execute(sql)
            rs = curs.fetchone()
            
        if (rs[0] == None):
            fromdate = '19900101'
        else:
            fromdate = rs[0]
        return fromdate
                
    def get_end_date(self):
        return stock.get_nearest_business_day_in_a_week(datetime.today().strftime(format="%Y%m%d"))
    
    def get_trade_dates(self, fromdate, todate):
        trade_dates = pd.DataFrame({'trade_date':stock.get_previous_business_days(fromdate=fromdate, todate=todate)})
        trade_busi_dates = trade_dates["trade_date"].dt.strftime("%Y%m%d").to_list()
        trade_busi_dates = [date for date in trade_busi_dates if (date != fromdate) & (date != todate)]
        return trade_busi_dates
    
    def get_data(self, trade_busi_date):
        ohlcv = stock.get_market_ohlcv_by_ticker(trade_busi_date, 'ALL')
        return ohlcv
    
    def replace_db(self, trade_busi_date):
        """병렬로 호출되는 함수: 각 호출마다 DB 연결이 독립적으로 생성됨"""
        data = self.get_data(trade_busi_date)
        if len(data) != 0:
            data['trade_busi_date'] = trade_busi_date
            # 새로운 DB 연결을 생성
            conn = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                db=db,
                                charset=charset)

            with conn.cursor() as curs:
                sql = f"""
                    INSERT INTO {self.table} (code, date, open, high, low, close, volume, value, pct_chg)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume),
                        value = VALUES(value),
                        pct_chg = VALUES(pct_chg)
                """
                data_to_insert = [
                    (ticker, row['trade_busi_date'], row['시가'], row['고가'], row['저가'], row['종가'], 
                    row['거래량'], row['거래대금'], row['등락률']) 
                    for ticker, row in data.iterrows()
                ]
                
                curs.executemany(sql, data_to_insert)
                conn.commit()
            
            conn.close()  # 프로세스별 DB 연결 종료
            print(f"{trade_busi_date}: 데이터 있음")
        else:
          print(f"{trade_busi_date}: 데이터 없음")

# %%
class DBUpdater_FUNDAMENTAL:
    def __init__(self):
        # 각 프로세스에서 DB 연결을 새로 생성
        self.conn = pymysql.connect(host=host,
                                    user=user,
                                    password=password,
                                    db=db,
                                    charset=charset)
        self.table = "market_fundamental"
        with self.conn.cursor() as curs:
            sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        code VARCHAR(20),
                        date DATE,
                        bps BIGINT(20),
                        per DOUBLE,
                        pbr DOUBLE,
                        eps BIGINT(20),
                        `div` DOUBLE,
                        dps BIGINT(20),
                        PRIMARY KEY (code, date)
                    )
                  """
            curs.execute(sql)
            
            self.conn.commit()

    def __del__(self):
        self.conn.close()
    
    def get_start_date(self):
        with self.conn.cursor() as curs:
            sql = f"SELECT max(date) FROM {self.table}"
            curs.execute(sql)
            rs = curs.fetchone()
            
        if (rs[0] == None):
            fromdate = '20000101'
        else:
            fromdate = rs[0]
        return fromdate
                
    def get_end_date(self):
        return stock.get_nearest_business_day_in_a_week(datetime.today().strftime(format="%Y%m%d"))
    
    def get_trade_dates(self, fromdate, todate):
        trade_dates = pd.DataFrame({'trade_date':stock.get_previous_business_days(fromdate=fromdate, todate=todate)})
        trade_busi_dates = trade_dates["trade_date"].dt.strftime("%Y%m%d").to_list()
        trade_busi_dates = [date for date in trade_busi_dates if (date != fromdate) & (date != todate)]
        return trade_busi_dates
    
    def get_data(self, trade_busi_date):
        fundamental = stock.get_market_fundamental_by_ticker(trade_busi_date, 'ALL')
        return fundamental
    
    def replace_db(self, trade_busi_date):
        """병렬로 호출되는 함수: 각 호출마다 DB 연결이 독립적으로 생성됨"""
        data = self.get_data(trade_busi_date)
        if len(data) != 0:
            data['trade_busi_date'] = trade_busi_date
            # 새로운 DB 연결을 생성
            conn = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                db=db,
                                charset=charset)

            with conn.cursor() as curs:
                sql = f"""
                    INSERT INTO {self.table} (code, date, bps, per, pbr, eps, `div`, dps)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        bps = VALUES(bps),
                        per = VALUES(per),
                        pbr = VALUES(pbr),
                        eps = VALUES(eps),
                        `div` = VALUES(`div`),
                        dps = VALUES(dps)
                """
                data_to_insert = [
                    (ticker, row['trade_busi_date'], row['BPS'], row['PER'], 
                     row['PBR'], row['EPS'], row['DIV'], row['DPS']) 
                    for ticker, row in data.iterrows()
                ]
                
                curs.executemany(sql, data_to_insert)
                conn.commit()
            
            conn.close()  # 프로세스별 DB 연결 종료
            print(f"{trade_busi_date}: 데이터 있음")
        else:
          print(f"{trade_busi_date}: 데이터 없음") 
          
          
class DBUpdater_CAP:
    def __init__(self):
        # 각 프로세스에서 DB 연결을 새로 생성
        self.conn = pymysql.connect(host=host,
                                    user=user,
                                    password=password,
                                    db=db,
                                    charset=charset)
        self.table = "market_cap"
        with self.conn.cursor() as curs:
            sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        code VARCHAR(20),
                        date DATE,
                        close BIGINT(20),
                        cap BIGINT(20),
                        volume BIGINT(20),
                        value BIGINT(20),
                        shares BIGINT(20),
                        PRIMARY KEY (code, date)
                    )
                  """
            curs.execute(sql)
            
            self.conn.commit()

    def __del__(self):
        self.conn.close()
    
    def get_start_date(self):
        with self.conn.cursor() as curs:
            sql = f"SELECT max(date) FROM {self.table}"
            curs.execute(sql)
            rs = curs.fetchone()
            
        if (rs[0] == None):
            fromdate = '19900101'
        else:
            fromdate = rs[0]
        return fromdate
                
    def get_end_date(self):
        return stock.get_nearest_business_day_in_a_week(datetime.today().strftime(format="%Y%m%d"))
    
    def get_trade_dates(self, fromdate, todate):
        trade_dates = pd.DataFrame({'trade_date':stock.get_previous_business_days(fromdate=fromdate, todate=todate)})
        trade_busi_dates = trade_dates["trade_date"].dt.strftime("%Y%m%d").to_list()
        trade_busi_dates = [date for date in trade_busi_dates if (date != fromdate) & (date != todate)]
        return trade_busi_dates
    
    def get_data(self, trade_busi_date):
        cap = stock.get_market_cap_by_ticker(trade_busi_date, 'ALL')
        return cap
    
    def replace_db(self, trade_busi_date):
        """병렬로 호출되는 함수: 각 호출마다 DB 연결이 독립적으로 생성됨"""
        data = self.get_data(trade_busi_date)
        if len(data) != 0:
            data['trade_busi_date'] = trade_busi_date
            # 새로운 DB 연결을 생성
            conn = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                db=db,
                                charset=charset)

            with conn.cursor() as curs:
                sql = f"""
                    INSERT INTO {self.table} (code, date, close, cap, volume, value, shares)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        close = VALUES(close),
                        cap = VALUES(cap),
                        volume = VALUES(volume),
                        value = VALUES(value),
                        shares = VALUES(shares)
                """
                data_to_insert = [
                    (ticker, row['trade_busi_date'], row['종가'], row['시가총액'], 
                     row['거래량'], row['거래대금'], row['상장주식수']) 
                    for ticker, row in data.iterrows()
                ]
                
                curs.executemany(sql, data_to_insert)
                conn.commit()
            
            conn.close()  # 프로세스별 DB 연결 종료
            print(f"{trade_busi_date}: 데이터 있음")
        else:
          print(f"{trade_busi_date}: 데이터 없음") 
#%%
if __name__ == '__main__':
    dbu = DBUpdater_OHLCV()
    
    fromdate = dbu.get_start_date()
    todate = dbu.get_end_date()
    trade_busi_dates = dbu.get_trade_dates(fromdate, todate)
    
    print(f"DBUpdater_OHLCV : Started at {time.strftime('%X')}")

    for trade_busi_date in trade_busi_dates:
        dbu.replace_db(trade_busi_date)

    print(f"DBUpdater_OHLCV : Finished at {time.strftime('%X')}")
    
    
    dbu2 = DBUpdater_FUNDAMENTAL()
    
    fromdate = dbu2.get_start_date()
    todate = dbu2.get_end_date()
    trade_busi_dates = dbu2.get_trade_dates(fromdate, todate)
      
    print(f"DBUpdater_FUNDAMENTAL : Started at {time.strftime('%X')}")

    for trade_busi_date in trade_busi_dates:
        dbu2.replace_db(trade_busi_date)

    print(f"DBUpdater_FUNDAMENTAL : Finished at {time.strftime('%X')}")
    
    
    dbu3 = DBUpdater_CAP()
    
    fromdate = dbu3.get_start_date()
    todate = dbu3.get_end_date()
    trade_busi_dates = dbu3.get_trade_dates(fromdate, todate)
      
    print(f"DBUpdater_CAP : Started at {time.strftime('%X')}")

    for trade_busi_date in trade_busi_dates:
        dbu3.replace_db(trade_busi_date)

    print(f"DBUpdater_CAP : Finished at {time.strftime('%X')}")