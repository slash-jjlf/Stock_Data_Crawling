# Data_Update 모듈로부터 kor_ticker 불러오기

from Data_Update import update_kor_ticker

kor_ticker = update_kor_ticker()
print(kor_ticker.head())

# Mysql에 업데이트

import pymysql

con = pymysql.connect(user='slash',
                      passwd='passion4',
                      host='192.168.0.53',
                      db='stock_db',
                      charset='utf8')

mycursor = con.cursor()
query = f"""
    insert into kor_ticker(종목코드, 종목명, 시장구분, 업종명, 종가, 시가총액, 기준일, EPS, PER, 선행EPS, 선행PER,
                           BPS, PBR, 주당배당금, 배당수익률, 종목구분)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s) as new
    on duplicate key update
    종목명=new.종목명, 시장구분=new.시장구분, 업종명=new.업종명, 종가=new.종가, 시가총액=new.시가총액, EPS=new.EPS,
    PER=new.PER, 선행EPS=new.선행EPS, 선행PER=new.선행PER, BPS=new.BPS, PBR=new.PBR, 주당배당금=new.주당배당금, 
    배당수익률=new.배당수익률, 종목구분=new.종목구분;
"""

args = kor_ticker.values.tolist()
mycursor.executemany(query, args)
con.commit()

con.close()


