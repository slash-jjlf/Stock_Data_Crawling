# 전종목 컨센서스 받아와서 Mysql에 저장하기

# 패키지 불러오기
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import requests as rq
import time
from tqdm import tqdm
import random
import json
import numpy as np

# DB 연결
engine = create_engine('mysql+pymysql://root:1234@localhost:3306/stock_db')
con = pymysql.connect(user='root',
                      passwd='1234',
                      host='127.0.0.1',
                      db='stock_db',
                      charset='utf8')
mycursor = con.cursor()

# 티커리스트 불러오기
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) and 종목구분 = '보통주'
and 시가총액 >= 50000000000000;
""", con=engine)

# DB 저장 쿼리
query1 = """
    insert into kor_con (항목, 기준일, 값, 종목코드, 종목명, 기간)
    values (%s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    값=new.값;
"""

query2 = """
    insert into kor_val (항목, 기준일, 값, 종목코드, 종목명, 기간)
    values (%s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    값=new.값;
"""

# 오류 발생시 저장할 리스트 생성
error_list = []

# 재무재표 클렛징 함수
def clean_data(raw_data, ticker, name, frequency):
    # 1) 원본 데이터로 데이터프레임 생성
    df = pd.DataFrame(raw_data['comp'])
    # 2) 필요한 행만 선택 (매출액, 영업이익)
    df = df.iloc[[0, 1, 4], 3:]
    # 3) 첫 번째 행을 컬럼명으로 지정 후 해당 행 삭제
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    # 4) 데이터를 긴 형식(long format)으로 변환
    df = pd.melt(df, id_vars='항목', var_name='기준일', value_name='값')
    # 5) '값' 열의 데이터 클리닝 및 타입 변환
    #    - 쉼표(,)를 제거하고 숫자 타입(정수)으로 변환
    df['값'] = df['값'].str.replace(',', '').replace(['', 'N/A'], np.nan).fillna(0).astype(float)
    # 6) 종목코드, 종목명, 기간 정보 추가
    df['종목코드'] = ticker
    df['종목명'] = name
    df['기간'] = frequency

    return df

# 루프 돌리기
for i in tqdm(range(0, len(ticker_list))):
              
    # 티커선택, 종목명
    ticker = ticker_list['종목코드'][i]
    name = ticker_list['종목명'][i]
    
    # 오류 발생시 이를 무시하고 다음 루프로 진행
    try:
        # 분기 데이터
        url_q = f'https://comp.fnguide.com/SVO2/json/data/01_06/01_A{ticker}_Q_D.json'
        response_q = rq.get(url_q)
        clean_text_q = response_q.content.decode('utf-8-sig')
        data_q = json.loads(clean_text_q)
        data_q_clean = clean_data(data_q, ticker, name, "Q")
        
        # 연간 데이터
        url_a = f'https://comp.fnguide.com/SVO2/json/data/01_06/01_A{ticker}_A_D.json'
        response_a = rq.get(url_a)
        clean_text_a = response_a.content.decode('utf-8-sig')
        data_a = json.loads(clean_text_a)
        data_a_clean = clean_data(data_a, ticker, name, "Y")
        
        # 데이터 병합
        data_fin = pd.concat([data_q_clean, data_a_clean])
        
        # 밸류지표
        def clean_data_v(raw_data, ticker, name, frequency):
            # 1) 원본 데이터로 데이터프레임 생성
            df = pd.DataFrame(raw_data['comp'])
            # 2) 필요한 행만 선택 (매출액, 영업이익)
            df = df.iloc[[0, 20, 21,22], 3:]
            # 3) 첫 번째 행을 컬럼명으로 지정 후 해당 행 삭제
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
            # 4) 데이터를 긴 형식(long format)으로 변환
            df = pd.melt(df, id_vars='항목', var_name='기준일', value_name='값')
            # 5) '값' 열의 데이터 클리닝 및 타입 변환
            #    - 쉼표(,)를 제거하고 숫자 타입(정수)으로 변환
            df['값'] = df['값'].str.replace(',', '').replace(['', 'N/A'], np.nan).fillna(0).astype(float)
            # 6) 종목코드, 종목명, 기간 정보 추가
            df['종목코드'] = ticker
            df['종목명'] = name
            df['기간'] = frequency

            return df

        data_v_clean = clean_data_v(data_a, ticker, name, "Y")
        
        # 재무제표 데이터를 DB에 저장
        args1 = data_fin.values.tolist()
        mycursor.executemany(query1, args1)
        
        args2 = data_v_clean.values.tolist()
        mycursor.executemany(query2, args2)
        
        con.commit()
        
    except:          
    # 오류 방생시 해당 종목명을 저장하고 다음 루프로 이동
        print(ticker)
        error_list.apppend(ticker)
        
        # 타입슬립 적용
    sleep_time = random.uniform(1, 5) # 타임습립을 1~5초 사이의 무작위 실수로 발생 시킴
    time.sleep(sleep_time)

engine.dispose()
con.close()

































