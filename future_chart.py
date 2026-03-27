import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine

from auth import BASE_URL, get_access_token
from future_master import (
    get_derivative_master,
    get_front_month,
    get_future_master,
    get_night_derivative_master,
)

def get_krx_holidays() -> set:
    """KRX 휴장일 set 반환"""
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME_FIN')}"
    )
    df = pd.read_sql("SELECT date FROM krx_holiday", engine)
    engine.dispose()
    if df.empty:
        return set()
    return set(pd.to_datetime(df["date"]).dt.date)


def _is_trading_day(d: date, holidays: set) -> bool:
    return d.weekday() < 5 and d not in holidays


def _prev_trading_day(d: date, holidays: set) -> date:
    d -= timedelta(days=1)
    while not _is_trading_day(d, holidays):
        d -= timedelta(days=1)
    return d


def get_trading_date(session: str, holidays: set) -> date:
    """현재시간 기준 가장 가까운 거래일의 base date 반환"""
    now = datetime.now()
    today = now.date()

    if session == "day":
        if _is_trading_day(today, holidays) and (now.hour > 8 or (now.hour == 8 and now.minute >= 45)):
            return today
        return _prev_trading_day(today, holidays)
    else:  # night
        if now.hour >= 18 and _is_trading_day(today, holidays):
            return today
        if now.hour < 6:
            return _prev_trading_day(today, holidays)
        return _prev_trading_day(today, holidays)


def format_time_column(df: pd.DataFrame, session: str, holidays: set) -> tuple[pd.Series, int]:
    """HHMMSS → YYYY-MM-DD HH:MM:SS 변환. 첫 번째 세션 경계 인덱스도 반환."""
    base = get_trading_date(session, holidays)
    times = df["time"].tolist()
    result = []
    boundary = len(times)

    for i, t in enumerate(times):
        h, m, s = int(t[:2]), int(t[2:4]), int(t[4:6])
        ti = int(t)

        if i > 0:
            prev_ti = int(times[i - 1])
            if session == "day":
                if ti > prev_ti:
                    boundary = i
                    break
            else:
                if prev_ti >= 180000 and ti < 180000:
                    boundary = i
                    break

        if session == "night" and h < 18:
            d = base + timedelta(days=1)
        else:
            d = base
        result.append(f"{d} {h:02d}:{m:02d}:{s:02d}")

    return pd.Series(result, index=df.index[:boundary]), boundary


# (tr_cd, InBlock명, OutBlock명)
TR_CONFIG = {
    "day": ("t2209", "t2209InBlock", "t2209OutBlock1"),
    "night": ("t8461", "t8461InBlock", "t8461OutBlock1"),
}


def get_future_chart(token: str, focode: str, session: str = "day", bgubun: int = 1, cnt: int = 740, holidays: set = None) -> pd.DataFrame:
    """선물 틱분별 차트 조회 (day=t2209, night=t8461)"""
    tr_cd, in_block, out_block = TR_CONFIG[session]

    url = f"{BASE_URL}/futureoption/chart"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "tr_cd": tr_cd,
        "tr_cont": "N",
        "tr_cont_key": "",
        "mac_address": "",
    }
    bgubun_val = str(bgubun) if session == "night" else bgubun
    body = {
        in_block: {
            "focode": focode,
            "cgubun": "B",
            "bgubun": bgubun_val,
            "cnt": cnt,
        }
    }

    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        print(f"  [{tr_cd} 오류] status={response.status_code}, body={response.text}")
        return pd.DataFrame()
    data = response.json()
    rows = data.get(out_block, [])
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["shcode"] = focode
    df["time"] = df["chetime"]
    if holidays is not None:
        time_series, boundary = format_time_column(df, session, holidays)
        df = df.iloc[:boundary].reset_index(drop=True)
        df["time"] = time_series
    for col in ["open", "high", "low"]:
        df[col] = pd.to_numeric(df[col])
    df["close"] = pd.to_numeric(df["price"])
    change = pd.to_numeric(df["change"])
    df["ch"] = change.where(df["sign"].isin(["1", "2", "3"]), -change)
    prev_close = df["close"] - df["ch"]
    df["chp"] = (df["ch"] / prev_close * 100).round(2)
    df["volume"] = pd.to_numeric(df["volume"])
    df["cvolume"] = pd.to_numeric(df["cvolume"])
    return df[["shcode", "time", "open", "high", "low", "close", "ch", "chp", "volume", "cvolume"]]


# 마스터 조회 매핑: (master함수, gubun, shcode라벨)
MASTER_CONFIG = {
    "kospi": (get_future_master, "", "KOSPI200F"),
    "kosdaq": (get_derivative_master, "SF", "KOSDAQ150F"),
    "night_kospi": (get_night_derivative_master, "NFU", "KOSPI200FN"),
    "night_kosdaq": (get_night_derivative_master, "NQF", "KOSDAQ150FN"),
}


def get_front_shcode(token: str, key: str) -> tuple[str, str, str]:
    """최근월물 단축코드/종목명/라벨 반환"""
    master_fn, gubun, label = MASTER_CONFIG[key]
    df = master_fn(token, gubun=gubun)
    front = get_front_month(df)
    return front.iloc[0]["shcode"], front.iloc[0]["hname"], label


if __name__ == "__main__":
    token_info = get_access_token()
    token = token_info["access_token"]
    holidays = get_krx_holidays()

    targets = [
        ("kospi", "day", ""),
        ("kosdaq", "day", ""),
        ("night_kospi", "night", " 야간"),
        ("night_kosdaq", "night", " 야간"),
    ]

    for i, (key, session, suffix) in enumerate(targets):
        if i > 0:
            time.sleep(2)
        shcode, hname, label = get_front_shcode(token, key)
        df = get_future_chart(token, focode=shcode, session=session, holidays=holidays)
        if df.empty:
            print("조회된 데이터가 없습니다.")
        else:
            df["shcode"] = label
            print(df)
