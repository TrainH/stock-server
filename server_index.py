# %%
import pandas as pd
import pymysql, time, json
from datetime import datetime

from pykrx import stock


# %%

class DBUpdater:
    def __init__(self):
        # 각 프로세스에서 DB 연결을 새로 생성
        self.conn = pymysql.connect(host='localhost',
                                    user='root',
                                    password='qja!$9rywns',
                                    db='Kindex',
                                    charset='utf8')
        with self.conn.cursor() as curs:
            sql = """
                    CREATE TABLE IF NOT EXISTS etf_price (
                        code VARCHAR(20),
                        date DATE,
                        open BIGINT(20),
                        high BIGINT(20),
                        low BIGINT(20),
                        close BIGINT(20),
                        volume BIGINT(20),
                        value BIGINT(20),
                        marketCap BIGINT(20),
                        index_value FLOAT,
                        PRIMARY KEY (code, date)
                    )
                  """
            curs.execute(sql)
            
            self.conn.commit()

    def __del__(self):
        self.conn.close()
    
    def get_start_date(self):
        with self.conn.cursor() as curs:
            sql = "SELECT max(date) FROM etf_price"
            curs.execute(sql)
            rs = curs.fetchone()
            
            if (rs[0] == None):
                fromdate = '20021014'
            else:
                fromdate = rs[0]
            return fromdate
                
    def get_end_date(self):
        return stock.get_index_ohlcv_by_ticker(datetime.today().strftime(format="%Y%m%d"))
    
    def get_trade_dates(self, fromdate, todate):
        trade_dates = pd.DataFrame({'trade_date':stock.get_previous_business_days(fromdate=fromdate, todate=todate)})
        trade_busi_dates = trade_dates["trade_date"].dt.strftime("%Y%m%d").to_list()
        trade_busi_dates = [date for date in trade_busi_dates if (date != fromdate) & (date != todate)]
        return trade_busi_dates
        
    def get_ohlcv(self, trade_busi_date):
        time.sleep(1)
        ohlcv = stock.get_etf_ohlcv_by_ticker(trade_busi_date)
        return ohlcv
    
    def replace_db(self, trade_busi_date):
        """병렬로 호출되는 함수: 각 호출마다 DB 연결이 독립적으로 생성됨"""
        data = self.get_ohlcv(trade_busi_date)
        if len(data) != 0:
            data['trade_busi_date'] = trade_busi_date
            # 새로운 DB 연결을 생성
            conn = pymysql.connect(host='localhost',
                                user='root',
                                password='qja!$9rywns',
                                db='etf',
                                charset='utf8')

            with conn.cursor() as curs:
                sql = """
                    INSERT INTO etf_price (code, date, open, high, low, close, volume, value, index_value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume),
                        value = VALUES(value),
                        index_value = VALUES(index_value)
                """
                data_to_insert = [
                    (ticker, row['trade_busi_date'], row['시가'], row['고가'], row['저가'], row['종가'], 
                    row['거래량'], row['거래대금'], row['기초지수']) 
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
    dbu = DBUpdater()
    
    fromdate = dbu.get_start_date()
    todate = dbu.get_end_date()
    trade_busi_dates = dbu.get_trade_dates(fromdate, todate)
      
    print(f"Started at {time.strftime('%X')}")

    for trade_busi_date in trade_busi_dates:
        dbu.replace_db(trade_busi_date)

    print(f"Finished at {time.strftime('%X')}")
