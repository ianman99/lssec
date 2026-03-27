import pandas as pd
import requests

from auth import BASE_URL, get_access_token


def get_kr_stock_list(token: str, gubun: str = "1") -> pd.DataFrame:
    """국내 주식 마스터 조회 (t9945)

    Args:
        gubun: 1=코스피(KSP), 2=코스닥(KSD)
    """
    url = f"{BASE_URL}/stock/market-data"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "tr_cd": "t9945",
        "tr_cont": "N",
        "tr_cont_key": "",
        "mac_address": "",
    }
    body = {
        "t9945InBlock": {
            "gubun": gubun,
        }
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data.get("t9945OutBlock", []))


if __name__ == "__main__":
    token_info = get_access_token()
    token = token_info["access_token"]

    kospi = get_kr_stock_list(token, gubun="1")
    
    kosdaq = get_kr_stock_list(token, gubun="2")

    all_stocks = pd.concat([kospi, kosdaq], ignore_index=True)
    print(f"\n총 {len(all_stocks)}건")
    print(all_stocks)
