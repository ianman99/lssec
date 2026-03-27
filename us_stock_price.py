import requests

from auth import BASE_URL, get_access_token

token_info = get_access_token()
token = token_info["access_token"]

url = f"{BASE_URL}/overseas-stock/market-data"

headers = {
    "content-type": "application/json; charset=utf-8",
    "authorization": f"Bearer {token}",
    "tr_cd": "g3101",
    "tr_cont": "N",
    "tr_cont_key": "",
    "mac_address": "",
}
body = {
    "g3101InBlock": {
        "delaygb" : "R",
        "keysymbol" : "82TSLA",
        "exchcd" : "82",
        "symbol" : "TSLA",    
    }
}

response = requests.post(url, headers=headers, json=body)
print("=== Response Header ===")
for key, value in response.headers.items():
    print(f"  {key}: {value}")
print("\n=== Response Body ===")
data = response.json()
print(data)
