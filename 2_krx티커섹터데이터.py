# 1. 영업일 날짜 데이터 네이버에서 가져오기

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

# 2. KRX 정보데이터시스템(data.krx.co.kr)에서 국내 주식 티커 및 섹터 데이터 크롤링

# STK(코스피) otp 발급---------------------------------------------------------

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


# KSQ(코스닥) otp 발급---------------------------------------------------------

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

# 3. 코스피/코스닥 데이터 합치기
krx_sector = pd.concat([sector_stk, sector_ksq])







