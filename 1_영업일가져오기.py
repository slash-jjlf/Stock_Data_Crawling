# 1) 영업일 날짜 데이터 네이버에서 가져오기

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


