import json
import ssl
import time

import pandas as pd
import websocket

from auth import get_access_token
from kr_stocks import get_kr_stock_list

WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket"


def get_all_shcodes(token: str) -> pd.DataFrame:
    """코스피+코스닥 전 종목 조회"""
    kospi = get_kr_stock_list(token, gubun="1")
    kosdaq = get_kr_stock_list(token, gubun="2")
    all_stocks = pd.concat([kospi, kosdaq], ignore_index=True)
    print(f"전 종목 조회 완료: 코스피 {len(kospi)}건 + 코스닥 {len(kosdaq)}건 = {len(all_stocks)}건")
    return all_stocks


def on_message(ws, message):
    # --- 트래픽 측정 ---
    msg_bytes = len(message.encode("utf-8"))
    ws.traffic_bytes += msg_bytes
    ws.traffic_msgs += 1

    now = time.time()
    elapsed = now - ws.traffic_last_time
    if elapsed >= 60:
        kb = ws.traffic_bytes / 1024
        total_kb = ws.traffic_total_bytes / 1024
        total_mb = total_kb / 1024
        mins = (now - ws.traffic_start_time) / 60
        timestamp = time.strftime("%H:%M:%S")
        print(
            f"[{timestamp}] "
            f"최근 {elapsed:.0f}초: {ws.traffic_msgs:,}건 / {kb:,.1f} KB | "
            f"누적 ({mins:.1f}분): {ws.traffic_total_msgs:,}건 / "
            f"{total_mb:,.2f} MB ({total_kb:,.1f} KB)"
        )
        ws.traffic_bytes = 0
        ws.traffic_msgs = 0
        ws.traffic_last_time = now

    data = json.loads(message)
    header = data.get("header", {})
    body = data.get("body", {})

    # 구독 응답
    if header.get("rsp_cd"):
        return

    # 체결 데이터 (출력 주석 처리)
    if header.get("tr_cd") == "US3" and body and body.get("price"):
        ws.traffic_total_bytes += msg_bytes
        ws.traffic_total_msgs += 1
    #     sign_map = {"1": "▲", "2": "▲", "3": " ", "4": "▼", "5": "▼"}
    #     sign = sign_map.get(body.get("sign", ""), " ")
    #     cgubun = "매수" if body.get("cgubun") == "+" else "매도"
    #     shcode = body.get("shcode", "")
    #     name = ws.code_map.get(shcode, shcode)
    #
    #     price = int(body["price"])
    #     change = int(body["change"])
    #     drate = float(body["drate"])
    #     cvolume = int(body["cvolume"])
    #
    #     # 등락 색상: 상승=빨강, 하락=파랑
    #     if sign in ("▲",):
    #         color = "\033[91m"
    #     elif sign in ("▼",):
    #         color = "\033[94m"
    #     else:
    #         color = "\033[0m"
    #     reset = "\033[0m"
    #     gray = "\033[90m"
    #
    #     t = body["chetime"]
    #     time_fmt = f"{t[:2]}:{t[2:4]}:{t[4:6]}"
    #
    #     print(
    #         f"{gray}{time_fmt}{reset} "
    #         f"{name:<12} "
    #         f"{color}{sign} {price:>10,}{reset} "
    #         f"{color}{change:>+8,} ({drate:>+6.2f}%){reset} "
    #         f"{cvolume:>8,}주 "
    #         f"{'[매수]' if cgubun == '매수' else '[매도]':>4} "
    #         f"{gray}{body.get('exchname', '')}{reset}"
    #     )


def on_error(ws, error):
    print(f"에러: {error}")


def on_close(ws, *args):
    print("\nWebSocket 연결 종료")


def on_open(ws):
    print("WebSocket 연결 완료. 구독 등록중...\n")
    codes = ws.shcodes
    for i, shcode in enumerate(codes):
        tr_key = f"U{shcode}".ljust(10)
        sub_msg = {
            "header": {
                "token": ws.token,
                "tr_type": "3",
            },
            "body": {
                "tr_cd": "US3",
                "tr_key": tr_key,
            },
        }
        ws.send(json.dumps(sub_msg))

        if (i + 1) % 100 == 0:
            print(f"  {i + 1} / {len(codes)} 종목 구독 완료")
            time.sleep(0.5)

    print(f"\n전 종목 {len(codes)}개 구독 완료. 실시간 체결 수신 대기중... (Ctrl+C 종료)\n")


def run_realtime_all(token: str, stocks: pd.DataFrame):
    """전 종목 실시간 체결 WebSocket 실행"""
    shcodes = stocks["shcode"].tolist()
    code_map = dict(zip(stocks["shcode"], stocks["hname"]))

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.token = token
    ws.shcodes = shcodes
    ws.code_map = code_map
    # 트래픽 측정 초기화
    ws.traffic_bytes = 0
    ws.traffic_msgs = 0
    ws.traffic_total_bytes = 0
    ws.traffic_total_msgs = 0
    ws.traffic_start_time = time.time()
    ws.traffic_last_time = time.time()
    ws.run_forever(
        sslopt={"cert_reqs": ssl.CERT_NONE},
        header={"Sec-WebSocket-Extensions": "permessage-deflate"},
    )


if __name__ == "__main__":
    token_info = get_access_token()
    token = token_info["access_token"]

    stocks = get_all_shcodes(token)
    run_realtime_all(token, stocks)
