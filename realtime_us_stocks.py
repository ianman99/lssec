import json
import ssl

import websocket

from auth import get_access_token

WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket"

# 나스닥 주요 30종목
SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "NFLX", "AMD", "QCOM", "ADBE", "INTC",
    "INTU", "CMCSA", "TXN", "AMGN", "AMAT",
    "MU", "LRCX", "MRVL", "KLAC", "SNPS",
    "CDNS", "CRWD", "MELI", "PANW", "COIN",
]


def on_message(ws, message):
    data = json.loads(message)
    header = data.get("header", {})
    body = data.get("body", {})

    if header.get("rsp_cd") or header.get("tr_cd") != "GSC" or not body.get("price"):
        return

    symbol = body.get("symbol", "").strip()
    sign_map = {"1": "▲", "2": "▲", "3": " ", "4": "▼", "5": "▼"}
    sign = sign_map.get(body.get("sign", ""), " ")

    price = float(body["price"])
    diff = float(body.get("diff", 0))
    rate = float(body.get("rate", 0))
    trdq = int(body.get("trdq", 0))
    totq = int(body.get("totq", 0))
    cgubun = "매수" if body.get("cgubun") == "+" else "매도"

    if sign == "▲":
        color = "\033[91m"
    elif sign == "▼":
        color = "\033[94m"
    else:
        color = "\033[0m"
    reset = "\033[0m"
    gray = "\033[90m"

    kortm = body.get("kortm", "")
    time_fmt = f"{kortm[:2]}:{kortm[2:4]}:{kortm[4:6]}" if len(kortm) >= 6 else kortm

    print(
        f"{gray}{time_fmt}{reset} "
        f"{symbol:<8} "
        f"{color}{sign} {price:>10.4f}{reset} "
        f"{color}{diff:>+8.4f} ({rate:>+6.2f}%){reset} "
        f"{trdq:>8,}주 [{cgubun}] "
        f"{gray}누적 {totq:,}{reset}"
    )


def on_error(ws, error):
    print(f"에러: {error}")


def on_close(ws, *args):
    print("\nWebSocket 연결 종료")


def on_open(ws):
    print(f"WebSocket 연결 완료. {len(SYMBOLS)}개 종목 구독중...\n")
    for symbol in SYMBOLS:
        sub_msg = {
            "header": {
                "token": ws.token,
                "tr_type": "3",
            },
            "body": {
                "tr_cd": "GSC",
                "tr_key": f"82{symbol}".ljust(18),
            },
        }
        ws.send(json.dumps(sub_msg))

    print(f"구독 완료: {', '.join(SYMBOLS)}")
    print("실시간 체결 수신 대기중... (Ctrl+C 종료)\n")


if __name__ == "__main__":
    token_info = get_access_token()
    token = token_info["access_token"]

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.token = token
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
