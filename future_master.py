import pandas as pd
import requests

from auth import BASE_URL, get_access_token


def get_future_master(token: str, gubun: str = "") -> pd.DataFrame:
    """지수선물 마스터 조회 (t8432)

    Args:
        gubun: V=변동성지수선물, S=섹터지수선물, 그 외=코스피200지수선물
    """
    url = f"{BASE_URL}/futureoption/market-data"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "tr_cd": "t8432",
        "tr_cont": "N",
        "tr_cont_key": "",
        "mac_address": "",
    }
    body = {
        "t8432InBlock": {
            "gubun": gubun,
        }
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data.get("t8432OutBlock", []))


def get_derivative_master(token: str, gubun: str = "SF") -> pd.DataFrame:
    """파생종목 마스터 조회 (t8435)

    Args:
        gubun: MF=미니선물, MO=미니옵션, WK=코스피200위클리옵션,
               SF=코스닥150선물, QW=코스닥150위클리옵션
    """
    url = f"{BASE_URL}/futureoption/market-data"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "tr_cd": "t8435",
        "tr_cont": "N",
        "tr_cont_key": "",
        "mac_address": "",
    }
    body = {
        "t8435InBlock": {
            "gubun": gubun,
        }
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data.get("t8435OutBlock", []))


def get_night_derivative_master(token: str, gubun: str = "NFU") -> pd.DataFrame:
    """KRX 야간파생 마스터 조회 (t8455)

    Args:
        gubun: NFU=KOSPI200선물, NMF=미니선물, NQF=코스닥150선물, NCF=상품선물,
               NOP=KOSPI200옵션, NMO=미니옵션, NQO=코스닥150옵션, NWO=위클리옵션
    """
    url = f"{BASE_URL}/futureoption/market-data"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "tr_cd": "t8455",
        "tr_cont": "N",
        "tr_cont_key": "",
        "mac_address": "",
    }
    body = {
        "t8455InBlock": {
            "gubun": gubun,
        }
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data.get("t8455OutBlock", []))


def get_front_month(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df["hname"].str.contains("SP")].iloc[:1]


if __name__ == "__main__":
    token_info = get_access_token()
    token = token_info["access_token"]

    cols = ["hname", "shcode", "expcode"]

    result = pd.concat([
        get_front_month(get_future_master(token, gubun="")),
        get_front_month(get_derivative_master(token, gubun="SF")),
        get_front_month(get_night_derivative_master(token, gubun="NFU")),
        get_front_month(get_night_derivative_master(token, gubun="NQF")),
    ], ignore_index=True)[cols]

    print("\n선물 최근월물")
    print(result.to_string(index=False))
