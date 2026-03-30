import os
import time
import threading
from datetime import date, datetime, timedelta

import pandas as pd
import requests
import socketio
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
    elif now.hour >= 18 and _is_trading_day(today, holidays):
        return today
    return _prev_trading_day(today, holidays)


def format_time_column(df: pd.DataFrame, session: str, holidays: set) -> tuple[pd.Series, int]:
    """HHMMSS → YYYY-MM-DD HH:MM:SS 변환. 첫 번째 세션 경계 인덱스도 반환."""
    base = get_trading_date(session, holidays)
    times = df["time"].tolist()
    result = []
    boundary = len(times)

    next_day = base + timedelta(days=1)
    is_night = session == "night"

    for i, t in enumerate(times):
        ti = int(t)

        if i > 0 and (
            (not is_night and ti > int(times[i - 1]))
            or (is_night and int(times[i - 1]) >= 180000 and ti < 180000)
        ):
            boundary = i
            break

        d = next_day if is_night and ti < 180000 else base
        result.append(f"{d} {ti // 10000:02d}:{ti // 100 % 100:02d}:{ti % 100:02d}")

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
    df["time"] = df["chetime"]

    # 미시작 세션 placeholder(volume=0) 제거
    if not df.empty and str(df.iloc[0]["volume"]) == "0":
        first_valid = df["volume"].astype(str).ne("0").idxmax()
        df = df.iloc[first_valid:].reset_index(drop=True)
        if df.empty:
            return pd.DataFrame()

    if holidays is not None:
        time_series, boundary = format_time_column(df, session, holidays)
        df = df.iloc[:boundary].reset_index(drop=True)
        df["time"] = time_series
    df["shcode"] = focode
    num_cols = ["open", "high", "low", "price", "change", "volume", "cvolume"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    df["close"] = df["price"]
    change = df["change"]
    df["ch"] = change.where(df["sign"].isin(["1", "2", "3"]), -change)
    df["chp"] = (df["ch"] / (df["close"] - df["ch"]) * 100).round(2)
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


LOCAL_SERVER_URL = os.getenv("SERVER_URL")
server_sio = socketio.Client()
reconnect_lock = threading.Lock()


@server_sio.event
def connect():
    print("로컬 서버 연결 성공")


@server_sio.event
def disconnect():
    print("로컬 서버 연결 해제")


def connect_local_websocket():
    time.sleep(1)
    while True:
        try:
            if server_sio.connected:
                break
            server_sio.connect(LOCAL_SERVER_URL, namespaces=["/host"])
            break
        except Exception as e:
            print(f"로컬 서버 연결 실패: {e}")
            try:
                server_sio.disconnect()
            except Exception:
                pass
            time.sleep(2)


def reconnect_local_websocket():
    with reconnect_lock:
        if server_sio.connected:
            return
        try:
            server_sio.disconnect()
        except Exception:
            pass
        connect_local_websocket()


def send_to_server(df: pd.DataFrame):
    try:
        data = df.to_dict(orient="records")
        server_sio.emit("indexFutureDataKor", data, namespace="/host")
    except Exception as e:
        print(f"데이터 전송 오류: {e}")
        reconnect_local_websocket()


if __name__ == "__main__":
    token_info = get_access_token()
    token = token_info["access_token"]
    token_issued = time.time()
    expires_in = token_info["expires_in"]
    holidays = get_krx_holidays()
    current_date = date.today()

    connect_local_websocket()

    targets = [
        ("kospi", "day"),
        ("kosdaq", "day"),
        ("night_kospi", "night"),
        ("night_kosdaq", "night"),
    ]

    while True:
        # 날짜 변경 시 holidays 갱신
        if date.today() != current_date:
            current_date = date.today()
            holidays = get_krx_holidays()

        # 토큰 만료 5분 전 재발급
        if time.time() - token_issued > expires_in - 600:
            token_info = get_access_token()
            token = token_info["access_token"]
            token_issued = time.time()
            expires_in = token_info["expires_in"]

        for i, (key, session) in enumerate(targets):
            if i > 0:
                time.sleep(2)
            try:
                shcode, hname, label = get_front_shcode(token, key)
                df = get_future_chart(token, focode=shcode, session=session, holidays=holidays)
                if df.empty:
                    print("조회된 데이터가 없습니다.")
                else:
                    df["shcode"] = label
                    print(df)
                    send_to_server(df)
            except Exception as e:
                print(f"[에러] {key} {session}: {e}")

        time.sleep(60)
