import os

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://openapi.ls-sec.co.kr:8080"

APP_KEY = os.getenv("LS_APP_KEY")
APP_SECRET_KEY = os.getenv("LS_SECRETKEY")


def get_access_token(app_key: str = APP_KEY, app_secret_key: str = APP_SECRET_KEY) -> dict:
    """LS증권 OpenAPI 접근토큰 발급"""
    url = f"{BASE_URL}/oauth2/token"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecretkey": app_secret_key,
        "scope": "oob",
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()


def revoke_access_token(
    token: str,
    app_key: str = APP_KEY,
    app_secret_key: str = APP_SECRET_KEY,
    token_type_hint: str = "access_token",
) -> dict:
    """LS증권 OpenAPI 접근토큰 폐기"""
    url = f"{BASE_URL}/oauth2/revoke"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
    }
    data = {
        "appkey": app_key,
        "appsecretkey": app_secret_key,
        "token_type_hint": token_type_hint,
        "token": token,
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    token_info = get_access_token()
    print(f"Access Token: {token_info['access_token']}")
    print(f"Token Type: {token_info['token_type']}")
    print(f"Expires In: {token_info['expires_in']}초")
