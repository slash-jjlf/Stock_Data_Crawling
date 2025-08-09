# KRX 티커/종목데이타 및 Fnguide 섹터 데이터 모두 업데이트

# 1. 영업일 날짜 데이터 네이버에서 가져오기----------------------------------------------------------------------

import requests as rq
from bs4 import BeautifulSoup

url = "https://finance.naver.com/sise/sise_deposit.naver"
data = rq.get(url)
data_html = BeautifulSoup(data.content)

parse_day = data_html.select_one('div.subtop_sise_graph2>ul.subtop_chart_note>li>span.tah').text
# 데이터가 여러개 나오기 때문에 select_one으로 하나만 가져옴
parse_day  # 숫자만 가져오기 위해 정규 표현식 사용해야함

import re
biz_day = re.findall('[0-9]+', parse_day)
biz_day =''.join(biz_day) # 리스트로 반환된 날짜데이터를 하나로 합침

biz_day #yyyymmdd 형태로 변환 완료

# 2. KRX 정보데이터시스템(data.krx.co.kr)에서 국내 주식 티커 및 섹터 데이터 크롤링--------------------------------

# STK(코스피) otp 발급

import requests as rq
from io import BytesIO
import pandas as pd

gen_otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
gen_otp_stk = {
    'mktId': 'STK', # SKT = KOSPI
    'trdDd': biz_day, # 네이버에서 가져온 최근 영업일
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
    }
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
# Referer, User-Agent 안하면 봇으로 인식해서 데이터 가져올 수 없음

otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
otp_stk # otp 취득 완료! 이제 다운로드 요청 하면됨

# 코스피 티커 및 섹터 데이터 가져오기
down_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
# 클렌징 처리
sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR') 
# 바이너리 형태로 변경 후, pandas로 읽어옴(기본 인코딩은 UTF8에서 EUC-KR로 변경)


# KSQ(코스닥) otp 발급

gen_otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
gen_otp_ksq = {
    'mktId': 'KSQ', # KSQ = KOSDAQ
    'trdDd': biz_day, # 네이버에서 가져온 최근 영업일
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
    }
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
# Referer, User-Agent 안하면 봇으로 인식해서 데이터 가져올 수 없음


otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text
otp_ksq # otp 취득 완료! 이제 다운로드 요청 하면됨

# 코스피 티커 및 섹터 데이터 가져오기
down_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
# 클렌징 처리
sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR') 
# 바이너리 형태로 변경 후, pandas로 읽어옴(기본 인코딩은 UTF8에서 EUC-KR로 변경)

# 3. 코스피/코스닥 데이터 합치기---------------------------------------------------------------------------------

krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop = True) # 데이터 합치고 인덱스 초기화
krx_sector['종목명'] = krx_sector['종목명'].str.strip() # 종목명에 공백 있는 경우 제거
krx_sector['기준일'] = biz_day # 최근 영업일로 기준일 열 추가

# 4. 개별 종목 지표 크롤링---------------------------------------------------------------------------------------

gen_otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
gen_otp_data = {
    'searchType': '1', 
    'mktId': 'ALL', # 코스피, 코스닥 동시에
    'trdDd': biz_day, # 네이버에서 가져온 최근 영업일
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
    }
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
# Referer, User-Agent 안하면 봇으로 인식해서 데이터 가져올 수 없음

otp_data = rq.post(gen_otp_url, gen_otp_data, headers=headers).text
otp_data # otp 취득 완료! 이제 다운로드 요청 하면됨

# 개별 종목 지표 데이터 가져오기
down_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
krx_ind = rq.post(down_url, {'code': otp_data}, headers=headers)
# 클렌징 처리
krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR') 
# 바이너리 형태로 변경 후, pandas로 읽어옴(기본 인코딩은 UTF8에서 EUC-KR로 변경)
krx_ind['종목명'] = krx_ind['종목명'].str.strip() # 종목명에 공백 있는 경우 제거
krx_ind['기준일'] = biz_day # 최근 영업일로 기준일 열 추가

# 5. 다운로드 받은 데이타 합치고 정리(krx_sector, krx_ind)-------------------------------------------------------

# 1) 두개의 데이터를 공통으로 존재하는 열을 기준으로 합침
kor_ticker = pd.merge(krx_sector, krx_ind,
                      on=krx_sector.columns.intersection(
                          krx_ind.columns).tolist(),
                      how='outer')

# 2) 하나의 데이터에만 존재하는 종목 확인
set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])) # 리츠 등  확인

# 3) 일반적인 종목과 스팩, 우선주, 리츠, 기타 주식을 구분
kor_ticker[kor_ticker['종목명'].str.contains('스팩|제[0-9]+호')]['종목명'] # 스펙 종목 찾기
kor_ticker[kor_ticker['종목코드'].str[-1:] != '0']['종목명'] # 종목코드 끝자리가 0이아니면 우선주
kor_ticker[kor_ticker['종목명'].str.endswith('리츠')]['종목명'] # 리츠 찾기

# 4) '2), 3)'의 조건들을 사용하여, 종목구분 열 추가

import numpy as np # np.where 함수 사용하기 위해 numpy 호출

diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명']))) # 한 곳에만 존재하는 종목

kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                              np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                                       np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                                                np.where(kor_ticker['종목명'].isin(diff), '기타', '보통주'))))

# 5) 최종 클렌징
kor_ticker = kor_ticker.reset_index(drop=True) # 인덱스 초기화
kor_ticker.columns = kor_ticker.columns.str.replace(' ', '') # 열이름에 공백 제거
kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '업종명', '종가', '시가총액', '기준일', 'EPS',
       'PER', '선행EPS', '선행PER', 'BPS', 'PBR', '주당배당금', '배당수익률', '종목구분']] # 원하는 열로만 재구성
kor_ticker = kor_ticker.replace({np.nan: None}) # mySQL에 nan을 저장하지 못하므로, None으로 변경

# 6. Mysql에 업데이트-------------------------------------------------------------------------------------------

import pymysql

# ubuntu_server
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

# ubuntu_gui
con = pymysql.connect(user='slash4444',
                      passwd='passion4',
                      host='192.168.0.125',
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

# FnGuide가 제공하는 섹터 분류 데이터 가져오기

# 1. 영업일 날짜 데이터 네이버에서 가져오기-------------------------------------------------------------------------

import requests as rq
from bs4 import BeautifulSoup

url = "https://finance.naver.com/sise/sise_deposit.naver"
data = rq.get(url)
data_html = BeautifulSoup(data.content)

parse_day = data_html.select_one('div.subtop_sise_graph2>ul.subtop_chart_note>li>span.tah').text
# 데이터가 여러개 나오기 때문에 select_one으로 하나만 가져옴
parse_day  # 숫자만 가져오기 위해 정규 표현식 사용해야함

import re
biz_day = re.findall('[0-9]+', parse_day)
biz_day =''.join(biz_day) # 리스트로 반환된 날짜데이터를 하나로 합침

biz_day #yyyymmdd 형태로 변환 완료

# 2. WICS 데이터 가져오기------------------------------------------------------------------------------------------

import json
import requests as rq
import pandas as pd

url = f'https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt=20250808&sec_cd=G10'
data = rq.get(url).json()

data_pd = pd.json_normalize(data['list']) # 데이터를 데이터프레임 형태로 변환

# 반복문 사용해서, 각 섹터 데이터 가져오기

import time
import json
import requests as rq
import pandas as pd
from tqdm import tqdm # 진행사항을 보여주는 모듈

sector_code = ['G25', 'G35', 'G50', 'G40', 'G10', 'G20', 'G55', 'G30', 'G15', 'G45']

data_sector =[] # 각 섹터에 대한 데이터 프레임을 쌓아 놓음

for i in tqdm(sector_code):
    url = url = f'https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd={i}'
    data = rq.get(url).json()
    data_pd = pd.json_normalize(data['list'])
    
    data_sector.append(data_pd)
    
    time.sleep(2)

# 반복문을 통해 수집한 각 섹터 데이터프레임들을 합치고 클렌징

kor_sector = pd.concat(data_sector, axis=0) # 리스트내에 쌓여있는 데이터프레임을 하나로 합침
kor_sector = kor_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']] # 필요한 열만 선택
kor_sector['기준일'] = biz_day
kor_sector['기준일'] = pd.to_datetime(kor_sector['기준일']) # 기준일을 데이트타임 형식으로 변환

# 3. Mysql에 업데이트----------------------------------------------------------------------------------------------

import pymysql

# ubuntu_server
con = pymysql.connect(user='slash',
                      passwd='passion4',
                      host='192.168.0.53',
                      db='stock_db',
                      charset='utf8')

mycursor = con.cursor()
query = f"""
    insert into kor_sector(IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
    values (%s,%s,%s,%s,%s) as new
    on duplicate key update
    IDX_CD=new.IDX_CD, CMP_KOR=new.CMP_KOR, SEC_NM_KOR=new.SEC_NM_KOR
"""

args = kor_sector.values.tolist()
mycursor.executemany(query, args)
con.commit()

con.close()

# ubuntu_gui
con = pymysql.connect(user='slash4444',
                      passwd='passion4',
                      host='192.168.0.125',
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


























