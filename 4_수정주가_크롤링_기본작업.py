# Mysql에서 종목데이터 불러오기

from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('mysql+pymysql://slash:passion4@192.168.0.53:3306/stock_db')
query = """
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) and 종목구분 = '보통주'; """  # 서브쿼리 사용

ticker_list = pd.read_sql(query, con=engine) # 판다스 read_sql 사용해서 데이터 가져오기
engine.dispose() # 반드시 접속 종료

# 주가 데이터 크롤링(from NAVER 증권)

from dateutil.relativedelta import relativedelta # 날짜를 만드는데 필요
import requests as rq
from io import BytesIO # 바이너리 데이터롤 변환
from datetime import date

# 루프 돌리기 위한 기본 작업
i = 0
ticker = ticker_list['종목코드'][i] # 루프 돌릴 티커 가져오기
name = ticker_list['종목명'][i]
fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d") # 시작일: 오늘 기준 5년전, 날짜도 yyyymmdd 문자열로 변환
to = (date.today()).strftime("%Y%m%d") # 종료일: 날짜는 yyyymmdd 문자열로 변환

url =(f"https://m.stock.naver.com/front-api/external/chart/domestic/info?symbol={ticker}&requestType=1\
      &startTime={fr}&endTime={to}&timeframe=day") # f스트링을 이용해서 url 작성

data = rq.get(url).content # 바이너리 형태로 데이터를 받아옴
data_price = pd.read_csv(BytesIO(data)) # data를 메모리안의 파일처럼 다룰 수 있게 해줌(파일 저장 없이 직접 데이터 처리 가능)

# 클렌징 처리
import re

price = data_price.iloc[:, 0:6] # 행전체와 필요한 열만 선택
price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량'] # 열이름이 약간씩 이상한 것 수정
price = price.dropna() # 마지막행에 n/a있는 것 삭제
price['날짜'] = price['날짜'].str.extract('(\d+)') # Regex를 이용해 날짜 부분에서 숫자만 추출
price['날짜'] = pd.to_datetime(price['날짜']) # 날짜에서 추출한 숫자를 datetime 형식으로 변환
price['종목코드'] = ticker
price['종목명'] = name
