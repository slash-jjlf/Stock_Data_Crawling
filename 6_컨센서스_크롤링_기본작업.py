# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 22:11:37 2025

@author: junil
"""

import json
import requests as rq
import pandas as pd
import numpy as np

# 1. 분기 데이터 추출
ticker = '005930'
name = '삼성전자'
url_q = f'https://comp.fnguide.com/SVO2/json/data/01_06/01_A{ticker}_Q_D.json'
response_q = rq.get(url_q)
clean_text_q = response_q.content.decode('utf-8-sig')
data_q = json.loads(clean_text_q)

# 2. 데이터 클리닝
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

data_q_clean = clean_data(data_q, ticker, name, "Q")

# 3. 연간 데이터 추출
url_a = f'https://comp.fnguide.com/SVO2/json/data/01_06/01_A{ticker}_A_D.json'
response_a = rq.get(url_a)
clean_text_a = response_a.content.decode('utf-8-sig')
data_a = json.loads(clean_text_a)

# 4. 데이터 클리닝
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

data_a_clean = clean_data(data_a, ticker, name, "Y")

# 5. 벨류에이션
url_a = f'https://comp.fnguide.com/SVO2/json/data/01_06/01_A{ticker}_A_D.json'
response_a = rq.get(url_a)
clean_text_a = response_a.content.decode('utf-8-sig')
data_a = json.loads(clean_text_a)
# 6. 데이터 클리닝
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

# 7. 데이터 병합
data_fin = pd.concat([data_q_clean, data_a_clean])