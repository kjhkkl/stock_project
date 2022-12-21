import requests
import FinanceDataReader as fdr
from bs4 import BeautifulSoup
import time

# 주식 업종 크롤러
def upjong_crawler() -> list:
    url = "https://finance.naver.com/sise/sise_group.naver?type=upjong" # naver 업종 url
    webpage = requests.get(url)
    soup = BeautifulSoup(webpage.content, "html.parser", from_encoding='cp949')
    upjong_html_code = soup.find('table').find_all('a')

    upjong_list = []
    for upjong in upjong_html_code:
        upjong_dict = {
            'upjong_name': upjong.text,
            'upjong_code': upjong.attrs['href'].split('no=')[1]
            }
        upjong_list.append(upjong_dict)

    return upjong_list

# 주식 정보 크롤러(kospi & kosdaq)
def stock_info_crawler(market_name: str) -> list:

    # 마켓(kospi & kosdaq)에 상장되 있는 주식 코드 추출
    stock_code_list = fdr.StockListing(market_name)['Code'].to_list()
    stock_info_list = []
    count = 0
    for code in stock_code_list[:5]:
        code_info_url = f"https://finance.naver.com/item/coinfo.naver?code={code}"
        webpage = requests.get(code_info_url)
        soup = BeautifulSoup(webpage.content, "html.parser", from_encoding='cp949')
        upjong_code = soup.find_all("a", {"class": "link_site"})[1]['href'].split('upjong&no=')[1]
        stock_name = soup.find(attrs={'class': 'wrap_company'}).find('a').text

        # 상장주식수 type(str -> int) 변환
        html_list = soup.find("div", {"class": "first"}).find_all('td')
        if (len(html_list) > 2):
            stock_amount = int(html_list[2].text.replace(",", ""))
        else:
            stock_amount = int(html_list[1].text.replace(",", ""))

        # 주식 개요 정리
        summary_info = soup.find(attrs={'class': 'summary_info'})
        stock_summary = ''
        if summary_info is not None:
            summary_info_list = summary_info.find_all('p')
            for i in range(len(summary_info_list)):
                if (i != len(summary_info_list) - 1):
                    stock_summary += f'{i + 1}.' + summary_info_list[i].text + '\n'
                else:
                    stock_summary += f'{i + 1}.' + summary_info_list[i].text
        else:
            stock_summary = None

        stock_dict = {
            'stock_code': code,
            'upjong_code': upjong_code,
            'market': market_name,
            'stock_name': stock_name,
            'stock_amount': stock_amount,
            'stock_summary': stock_summary,
            'exist_or_del': True
        }
        stock_info_list.append(stock_dict)

        # request 시간 딜레이 및 남은 크롤러 페이지
        time.sleep(1)
        count += 1
        print(f"남은 데이터 = {len(stock_code_list) - count}")

    return stock_info_list

