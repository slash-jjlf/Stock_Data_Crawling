# 전 종목 재무제표를 받아와서, mysql에 저장하기

# 패키지 불러오기
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
import re
import time
from tqdm import tqdm
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
    insert into kor_fs (계정, 기준일, 값, 종목코드, 종목명, 공시구분)
    values (%s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    값=new.값;
"""

# 오류 발생시 저장할 리스트 생성
error_list = []

# 재무재표 클렛징 함수
def clean_fs(df, ticker, name, frequency):

    df = df[~df.loc[:, ~df.columns.isin(['계정'])].isna().all(axis=1)] # 모든열이 na에 해당하는 부분 없애기(적어도 데이터가 한개있는 열은 추출)
    df = df.drop_duplicates(['계정'], keep='first') # 계정에서 중복된 항목들 제거하고, 첫번째 위치하는 데이터만 유지
    df = pd.melt(df, id_vars='계정', var_name='기준일', value_name ='값') # 기준일을 기준으로 데이터들을 재정렬
    df = df[~pd.isnull(df['값'])] # 멜트로 재정렬하고 값이 na가 아닌 것들만 추출
    df['계정'] = df['계정'].replace({'계산에 참여한 계정 펼치기': ''}, regex=True) #  df의 계정 열에서 '계 참 계 펼치기' 삭제
    df['기준일'] = pd.to_datetime(df['기준일'], format='%Y/%m') + pd.tseries.offsets.MonthEnd() # 데이트타임으로 바꾸면서, 31일 월말 기준으로 바꿈
    df['종목코드'] = ticker
    df['종목명'] = name
    df['공시구분'] = frequency
    
    return df

# 루프 돌리기
for i in tqdm(range(0, len(ticker_list))):
              
    # 티커선택, 종목명
    ticker = ticker_list['종목코드'][i]
    name = ticker_list['종목명'][i]
    
    # 오류 발생시 이를 무시하고 다음 루프로 진행
    try:
        
        # url 생성 및 데이터 받아오기
        url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}"
        data = pd.read_html(url, displayed_only=False) # 숨겨진 항목도 받아오기 위해 False 부분 추가
        
        # 연간 데이터---------------------------------------------------------------------------------
        data_fs_y = pd.concat([data[0].iloc[:, ~data[0].columns.str.contains("전년동기")],
                   data[2], data[4]]) # 연간 손익 계산서만 전년 동기들어간 열 제외
        data_fs_y=data_fs_y.rename(columns={data_fs_y.columns[0]:'계정'}) # 열 인덱스 첫열 이름을 계정으로 통일
    
        # 결산월에 맞춰, 연간 데이터만 남기기(분기 데이터가 들어와 있기 때문에)
        page_data = rq.get(url)
        page_data_html = BeautifulSoup(page_data.content, 'lxml')
        
        fiscal_data = page_data_html.select('div.corp_group1 > h2')[1].text
        fiscal_data_text = re.findall('[0-9]+', fiscal_data) # 결산월 str로 뽑아냄
        data_fs_y = data_fs_y.loc[:, (data_fs_y.columns == '계정') | 
                          (data_fs_y.columns.str[-2:].isin(fiscal_data_text)
                          )] # 열이름에 계정이거나 마지막 두글자가 결산월과 같은 열만 추출
        
        # 클렌징 함수에 집어 넣고 연간 재무제표 데이터 획득
        data_fs_y_clean = clean_fs(data_fs_y, ticker, name, 'y')
        
        # 분기 데이터---------------------------------------------------------------------------------
        data_fs_q = pd.concat([data[1].iloc[:, ~data[1].columns.str.contains("전년동기")],
                   data[3], data[5]]) # 분기 손익 계산서만 전년 동기들어간 열 제외
        data_fs_q=data_fs_q.rename(columns={data_fs_q.columns[0]:'계정'}) # 열 인덱스 첫열 이름을 계정으로 통일
    
        # 분기데이터는 결산월 작업을 안해도 됨 -> 바로 클렌징 함수 처리
        data_fs_q_clean = clean_fs(data_fs_q, ticker, name, 'q')
    
        # 연간/분기 데이터 합치기
        data_fs_bind = pd.concat([data_fs_y_clean, data_fs_q_clean])
        
        # 재무제표 데이터를 DB에 저장
        args = data_fs_bind.values.tolist()
        mycursor.executemany(query, args)
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


                  
                  








