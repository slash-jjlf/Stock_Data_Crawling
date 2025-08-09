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
