# 패키지 불러오기
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# DB 연결
engine = create_engine('mysql+pymysql://slash:passion4@192.168.0.53:3306/stock_db')
con = pymysql.connect(user='slash',
                      passwd='passion4',
                      host='192.168.0.53',
                      db='stock_db',
                      charset='utf8')
mycursor = con.cursor()

# 분기 재무재표 불러오기
kor_fs = pd.read_sql("""
select * from kor_fs
where 공시구분 = 'q'
and 계정 in ('당기순이익', '자본', '영업활동으로인한현금흐름', '매출액');
""", con=engine)

# 티커 리스트 불러오기
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker)
and 종목구분 = '보통주';
""", con=engine)

engine.dispose()

# TTM(tr 12m) 구하기
kor_fs = kor_fs.sort_values(['종목코드', '계정', '기준일']) # 데이터 소팅
kor_fs['ttm'] = kor_fs.groupby(
    ['종목코드', '계정'], as_index=False)['값'].rolling(window=4,
                                                 min_periods=4).sum()['값'] # 4개분기 값 더하기

# 자본은 평균 구하기
kor_fs['ttm'] = np.where(kor_fs['계정'] == '자본', 
                            kor_fs['ttm'] / 4, kor_fs['ttm']) # 자본계정 4개분기 합친거 다시 4로 나누기
kor_fs = kor_fs.groupby(['종목코드', '계정']).tail(1) # 그룹으로 묶은 후, 가장 최근(마지막) 숫자만 

# 필요한 데이터(지표 계산에)와 시가총액은 ticker_list로 부터 가져와 merge
kor_fs_merge = kor_fs[['계정', '종목코드', '종목명', 'ttm']].merge(
    ticker_list[['종목코드', '시가총액', '기준일']], on = '종목코드')
# 시가총액 부분을 억으로 나눠워서 계산 스케일을 맞춤
kor_fs_merge['시가총액'] = kor_fs_merge['시가총액']/100000000

# 밸류에이션 계산(tr 12m)
kor_fs_merge['value'] = kor_fs_merge['시가총액'] / kor_fs_merge['ttm']
kor_fs_merge['value'] = kor_fs_merge['value'].round(4)
kor_fs_merge['지표'] = np.where(
    kor_fs_merge['계정']=='매출액', 'PSR',
    np.where(
        kor_fs_merge['계정']=='영업활동으로인한현금흐름', 'PCR',
        np.where(kor_fs_merge['계정']=='당기순이익', 'PER',
                 np.where(kor_fs_merge['계정']=='자본', 'PBR', None))))

# 최종 클렌징
kor_fs_merge.rename(columns={'value': '값'}, inplace=True) # 열이름 값으로 변경                  
kor_fs_merge = kor_fs_merge[['종목코드', '종목명', '기준일', '지표', '값']]
kor_fs_merge = kor_fs_merge.replace([np.inf, -np.inf, np.nan], None) # inf나 nan을 None으로 변경

# Sql에 Upsert
query = """
insert into kor_TrVal (종목코드, 종목명, 기준일, 지표, 값)
values (%s, %s, %s, %s, %s) as new
on duplicate key update
값 = new.값
"""

args_fs = kor_fs_merge.values.tolist()
mycursor.executemany(query, args_fs)
con.commit()

# 배당 수익률
ticker_list['배당수익률']=ticker_list['값'].round(4)
ticker_list.rename(columns={'배당수익률': '값'}, inplace=True)
ticker_list['지표'] = 'DY'
dy_list = ticker_list[['종목코드', '종목명', '기준일', '지표', '값']]
dy_list = dy_list.replace([np.inf, -np.inf, np.nan], None)                                            
dy_list = dy_list[dy_list['값'] != 0]

args_dy = dy_list.values.tolist()
mycursor.executemany(query, args_dy)
con.commit()                                                 

engine.dispose()
con.close()