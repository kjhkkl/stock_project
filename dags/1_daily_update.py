from bs4 import BeautifulSoup
import datetime
import requests
import json
import time
import re
import pandas as pd
import numpy as np
import pymysql
import sys
import pendulum
import FinanceDataReader as fdr
from pykrx import stock
from pykrx import bond
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import DAG

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "start_date": datetime.datetime(2022, 12, 19, tzinfo=KST)
}

# DB config
USER = Variable.get("mysql_user")
PWD = Variable.get("mysql_pwd")
HOST = Variable.get("mysql_host")

# DB 커넥터
def get_connect_db(db_name: str):
    conn = pymysql.connect(host = HOST,
                     database = f'{db_name}',
                     user = USER,
                     password = PWD)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor
    
# 코스피로 이전상장된 코스닥 상장 회사 유무 확인
def kosdaq_to_kospi(code_list: list[str]):
    conn, cursor = get_connect_db('stock')
    for stock_code in code_list:
        select_sql = f'''
            SELECT EXISTS(
                SELECT
                    *
                FROM
                    table_stock_classification
                WHERE 
                    stock_code = '{stock_code}' and
                    market = 'kosdaq'
            )as success;
            '''
        cursor.execute(select_sql)
        status = cursor.fetchall()[0]['success']
    
        if (status == 1):
            update_sql = f'''
                UPDATE 
                    table_stock_classification
                SET 
                    market = 'kospi'
                WHERE 
                    stock_code = {stock_code}
                '''
        else:
            pass
    cursor.close()
    conn.close()

# 코스피 or 코스닥 시장 업데이트
def update_stock_market(market_name: str):
    
    # 상장된 주식 code번호 리스트
    today_stock_code_list = fdr.StockListing(market_name)['Code'].to_list()
    
    if(market_name == 'kospi'): 
        kosdaq_to_kospi(today_stock_code_list)
    else:
        pass
    
    # 코스피 or 코스닥 변동사항 확인
    conn, cursor = get_connect_db('stock')
    select_sql = f'''
            SELECT 
                stock_code 
            FROM 
                table_stock_classification
            WHERE market = '{market_name}' 
            AND exist_or_del = True;
            '''
    cursor.execute(select_sql)
    db_stock_code = cursor.fetchall()
    db_stock_code_list = [code['stock_code'] for code in db_stock_code]
    cursor.close()
    conn.close()
    
    # 새로 상장된 코드 리스트와 상장 폐지된 코드 리스트 추출
    new_stock_code_list = list(set(today_stock_code_list) - set(db_stock_code_list))
    delete_stock_code_list = list(set(db_stock_code_list) - set(today_stock_code_list))
    
    # 상장폐지 목록 업데이트
    if not delete_stock_code_list:
        pass
    else:
        conn, cursor = get_connect_db('stock')
        for stock_code in delete_stock_code_list:
            update_sql = f'''
                UPDATE table_stock_classification
                SET exist_or_del = False
                WHERE stock_code = '{stock_code}'
                '''
            cursor.execute(update_sql)
            conn.commit()
        cursor.close()
        conn.close()        
    
    # 상장 목록 업데이트
    if not new_stock_code_list:
        pass
    else:
        conn, cursor = get_connect_db('stock')
        new_stock_dict_list = []
        for code in new_stock_code_list:
            code_info_url = f"https://finance.naver.com/item/coinfo.naver?code={code}"
            webpage = requests.get(code_info_url)
            soup = BeautifulSoup(webpage.content, "html.parser", from_encoding='cp949')
            upjong_code = soup.find_all("a",{"class":"link_site"})[1]['href'].split('upjong&no=')[1]
            stock_name = soup.find(attrs = {'class':'wrap_company'}).find('a').text
            html_list = soup.find("div",{"class":"first"}).find_all('td')
            if(len(html_list) > 2):
                stock_amount = int(html_list[2].text.replace(",",""))
            else:
                stock_amount = int(html_list[1].text.replace(",",""))
            summary_info = soup.find(attrs = {'class':'summary_info'})
            stock_summary = ''
            if summary_info is not None:
                stock_info_list = summary_info.find_all('p')
                for i in range(len(stock_info_list)):
                    if(i != len(stock_info_list) -1):
                        stock_summary += f'{i+1}.' + stock_info_list[i].text + '\n'
                    else:
                        stock_summary += f'{i+1}.' + stock_info_list[i].text
            else:
                stock_summary = None
            
            stock_dict = {
                'stock_code' : code,
                'upjong_code' : upjong_code,
                'market' : market_name,
                'stock_name' : stock_name,
                'stock_amount' : stock_amount,
                'stock_summary' : stock_summary,
                'exist_or_del' : True
            }
            new_stock_dict_list.append(stock_dict)
        
        for data in new_stock_dict_list:
            insert_sql = '''
                INSERT INTO table_stock_classification
                VALUES(%(stock_code)s,%(upjong_code)s,%(market)s,%(stock_name)s,%(stock_amount)s,%(stock_summary)s,%(exist_or_del)s);
                '''
            cursor.execute(insert_sql, data)
            conn.commit()
        
        cursor.close()
        conn.close()
        
# table내의 가장 최신 날짜 추출
def table_last_date() -> str:
    conn, cursor = get_connect_db('stock')
    select_sql = '''
            SELECT
                closing_date
            FROM 
                table_stock_flow
            ORDER BY 
                closing_date desc limit 1;
            '''
    cursor.execute(select_sql)
    last_date = (cursor.fetchall()[0]['closing_date'] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    cursor.close()
    conn.close()
    
    return last_date

# 날짜별 매매 데이터 넣기
def insert_stock_flow(market_name :str, start_date: str):
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    
    # 주식시장 열린 날짜만 추출
    date_list = fdr.DataReader('005930', start_date, today).reset_index()['Date'].tolist()
    
    for date in date_list:
        search_date = date.strftime('%Y%m%d')
        stock_df = stock.get_market_ohlcv(search_date, market_name).reset_index()
        market_cap_df = stock.get_market_cap(search_date).reset_index()
        stock_dict = stock_df.to_dict()
        stock_flow_list = []
        for i in range(len(stock_df)):
            daily_stock_dict = {
                    'stock_code' : stock_dict['티커'][i],
                    'open_price' : stock_dict['시가'][i],
                    'close_price' : stock_dict['종가'][i],
                    'high_price' : stock_dict['고가'][i],
                    'low_price' : stock_dict['저가'][i],
                    'volumn' : stock_dict['거래량'][i],
                    'change_per' : round(stock_dict['등락률'][i], 3),
                    'closing_date' : date.strftime('%Y-%m-%d'),
                    'market_cap' : market_cap_df[market_cap_df['티커'] == stock_dict['티커'][i]]['시가총액'].to_list()[0]
                    }
            stock_flow_list.append(daily_stock_dict)
            
        conn, cursor = get_connect_db('stock')
        for data in stock_flow_list:
            insert_sql = '''
                INSERT INTO 
                    table_stock_flow
                VALUES(%(stock_code)s,%(open_price)s,%(close_price)s,%(high_price)s,
                %(low_price)s,%(volumn)s,%(change_per)s,%(closing_date)s,%(market_cap)s);
                '''
            cursor.execute(insert_sql, data)
            conn.commit()
    
        cursor.close()
        conn.close()
    
@dag(dag_id = '1.daily_update_pipeline',
    schedule_interval = "0 22 * * *",
    default_args = default_args,
    tags = ['daily', 'stock_flow'],
    catchup = False)

def taskflow():
    @task(multiple_outputs = True)
    def set_market():
        return {'kospi' : 'kospi', 'kosdaq' : 'kosdaq'}
    
    @task
    def kospi_update(market_name: str):
        update_stock_market(market_name)
        return market_name.upper()
    
    @task
    def kosdaq_update(market_name: str):
        update_stock_market(market_name)
        return market_name.upper()
    
    @task
    def search_start_date():
        last_date = table_last_date()
        return last_date
    
    @task
    def kospi_daily_data_insert(market_name: str, last_date: str):
        insert_stock_flow(market_name, last_date)
        
    @task
    def kosdaq_daily_data_insert(market_name: str, last_date: str):
        insert_stock_flow(market_name, last_date)
        
    set_market_data = set_market()
    search_date = search_start_date()
    update_kospi = kospi_update(set_market_data['kospi'])
    update_kosdaq = kosdaq_update(set_market_data['kosdaq'])
    insert_kospi = kospi_daily_data_insert(update_kospi, search_date)
    insert_kosdaq = kosdaq_daily_data_insert(update_kosdaq, search_date)
    
double_pipeline = taskflow()
   

