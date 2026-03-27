import pandas as pd
import requests

from auth import BASE_URL, get_access_token



"""해외주식 마스터 조회 (g3190) - 연속조회로 전체 종목 수집

Args:
token: 접근토큰
natcode: 국가코드 (US, CN, HK, JP, VN 등)
exgubun: 거래소구분 (1=홍콩, 2=미국전체, 3=상해, 4=심천, 5=도쿄, 6=하노이, 7=호치민)
readcnt: 한 번에 조회할 건수
"""

url = f"{BASE_URL}/overseas-stock/market-data"
token_info = get_access_token()
token = token_info["access_token"]

headers = {
    "content-type": "application/json; charset=utf-8",
    "authorization": f"Bearer {token}",
    "tr_cd": "g3190",
    "tr_cont": "N",
    "tr_cont_key": "",
    "mac_address": "",
}
body = {
    "g3190InBlock": {
        "delaygb": "R",
        "natcode": "US",
        "exgubun": "2",
        "readcnt": 100,
        "cts_value": "",
    }
}

response = requests.post(url, headers=headers, json=body)
data = response.json()
print(data)



