# 패키지 불러오기
from sqlalchemy import create_engine
import pandas as pd

# DB 연결
engine = create_engine('mysql+pymysql://slash:passion4@192.168.0.53:3306/stock_db')

# 티커 리스트
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker)
and 종목구분 = '보통주';
""", con=engine)

# 삼성전자 분기 재무재표
sample_fs = pd.read_sql("""
select * from kor_fs
where 공시구분 = 'q'
and 종목코드 = '005930'
and 계정 in ('당기순이익', '자본', '영업활동으로인한현금흐름', '매출액');
""", con=engine)

engine.dispose()

sample_fs = sample_fs.sort_values(['종목코드', '계정', '기준일'])

# 4개 분기 계정 숫자 합침
sample_fs['ttm'] = sample_fs.groupby(
    ['종목코드', '계정'], as_index=False)['값'].rolling(window=4,
                                                 min_periods=4).sum()['값']

# 데이터 클린징
import numpy as np                                         

sample_fs['ttm'] = np.where(sample_fs['계정'] == '자본', 
                            sample_fs['ttm'] / 4, sample_fs['ttm']) # 자본계정 4개분기 합친거 다시 4로 나누기
sample_fs = sample_fs.groupby(['종목코드', '계정']).tail(1) # 그룹으로 묶은 후, 가장 최근(마지막) 숫자만 남기기

# 필요한 데이터(지표 계산에)와 시가총액은 ticker_list로 부터 가져와 merge
sample_fs_merge = sample_fs[['계정', '종목코드', '종목명', 'ttm']].merge(
    ticker_list[['종목코드', '시가총액', '기준일']], on = '종목코드')
# 시가총액 부분을 억으로 나눠워서 계산 스케일을 맞춤
sample_fs_merge['시가총액'] = sample_fs_merge['시가총액']/100000000

# 밸류에이션 계산(tr 12m)
sample_fs_merge['value'] = sample_fs_merge['시가총액'] / sample_fs_merge['ttm']
sample_fs_merge['지표'] = np.where(
    sample_fs_merge['계정']=='매출액', 'PSR',
    np.where(
        sample_fs_merge['계정']=='영업활동으로인한현금흐름', 'PCR',
        np.where(sample_fs_merge['계정']=='당기순이익', 'PER',
                 np.where(sample_fs_merge['계정']=='자본', 'PBR', None))))

# 배당 수익률 계산
ticker_list_sample = ticker_list[ticker_list['종목코드']=='005930'].copy()
ticker_list_sample['DY'] = ticker_list_sample['주당배당금'] / ticker_list_sample['종가']














