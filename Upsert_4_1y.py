# 수정 주가를 크롤링하고, mysql에 저장하기

# 패키지 불러오기
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
import requests as rq
import time
from tqdm import tqdm
from io import BytesIO
import random

# DB 연결
engine = create_engine('mysql+pymysql://slash:passion4@192.168.0.53:3306/stock_db')
con = pymysql.connect(user='slash',
                      passwd='passion4',
                      host='192.168.0.53',
                      db='stock_db',
                      charset='utf8')
mycursor = con.cursor()

# 티커리스트 불러오기
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) and 종목구분 = '보통주';
""", con=engine)

# DB 저장 쿼리
query = """
    insert into kor_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드, 종목명)
    values (%s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    시가= new.시가, 고가= new.고가, 저가= new.저가, 종가= new.종가, 거래량= new.거래량, 종목명= new.종목명;
"""

# 오류 발생시 저장 리스트
error_list = []

# 전종목 주가 다운로드 및 저장
for i in tqdm(range(0, len(ticker_list))):

    # 티커선택
    ticker = ticker_list['종목코드'][i]
    name = ticker_list['종목명'][i] # 데이터에 추가할 종목명 가져오기
    
    # 시작일과 종료일
    fr = (date.today() + relativedelta(years=-1)).strftime("%Y%m%d") # 날짜를 yyyymmdd 문자열로 변환
    to = (date.today()).strftime("%Y%m%d") # 날짜를 yyyymmdd 문자열로 변환
    
    # 오류 발생 시 이를 무시하고 다음 루프로 진행
    try:
    
        # url 생성
        url =(f"https://m.stock.naver.com/front-api/external/chart/domestic/info?symbol={ticker}&requestType=1\
              &startTime={fr}&endTime={to}&timeframe=day")
        
        # 데이터 다운로드
        data = rq.get(url).content # 바이너리 형태로 데이터를 받아옴
        data_price = pd.read_csv(BytesIO(data)) # ByteIO로 파일 저장 없이 직접 데이터 처리 가능
        
        # 데이터 클렌징    
        price = data_price.iloc[:, 0:6] # 행전체와 필요한 열만 선택
        price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량'] # 열이름이 약간씩 이상한 것 수정
        price = price.dropna() # 마지막행에 n/a있는 것 삭제
        price['날짜'] = price['날짜'].str.extract('(\d+)') # Regex를 이용해 날짜 부분에서 숫자만 추출
        price['날짜'] = pd.to_datetime(price['날짜']) # 날짜에서 추출한 숫자를 datetime 형식으로 변환
        price['종목코드'] = ticker
        price['종목명'] = name
        
        # 주가 데이터를 DB에 저장
        args = price.values.tolist()
        mycursor.executemany(query, args)
        con.commit()
    
    except:
        # 오류 발생시 error_list에 티커 저장하고 넘어가기
        print(ticker)
        error_list.append(ticker)
    
    # 타임슬립 적용
    sleep_time = random.uniform(1, 5) # 타임습립을 1~5초 사이의 무작위 실수로 발생 시킴
    time.sleep(sleep_time)

# DB 연결 종료
engine.dispose()
con.close()
























