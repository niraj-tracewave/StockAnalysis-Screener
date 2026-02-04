import asyncio
import json

from app.core.angel_ws import AngelWSClient
from app.apis.v1.websockets.manager import manager

# angel = AngelWSClient(
#     client_id="6e560bba-4d04-464c-ac06-7e5c9c508792",
#     access_token="eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkQ0NjI0NDkiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeUxDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaU9EQmxOalF6TURndE16bGlaUzB6TkdWa0xXRmpOamd0TmpFNU56ZzFORFk1TXpBM0lpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXlJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVElzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOUxDSnRaaUk2ZXlKemRHRjBkWE1pT2lKaFkzUnBkbVVpZlgwc0ltbHpjeUk2SW5SeVlXUmxYMnh2WjJsdVgzTmxjblpwWTJVaUxDSnpkV0lpT2lKRU5EWXlORFE1SWl3aVpYaHdJam94TnpZNE9Ua3hNak15TENKdVltWWlPakUzTmpnNU1EUTJOVElzSW1saGRDSTZNVGMyT0Rrd05EWTFNaXdpYW5ScElqb2lPVEV6TVdNelptTXROemhtTkMwME1qSTRMV0U1TlRrdE9XSTFZelJoTldWa056RTNJaXdpVkc5clpXNGlPaUlpZlEuRGZ0ci1ldFI5MF94MzQ4eUt5aTUtQ1ZReE8wQVM2bm9FLXRSWXczYWRMUG1ZeVlkbE5VaFluNmNOeTY4ZFhXX2FuZTZPbm5PcjdBRmlpMEdXVlU4aGx4NHhHVzFGbWlsYVlldjNUMFB3X0ZZZTlXWkM3UmdGdWY2VXZiWUw5cy1jZ3lkWHhHcHZURW44ZXBTQzRMYmpRMWJwN0Vlc0tkQlpFTzFvZTZielVzIiwiQVBJLUtFWSI6Im0xQWs2emV6IiwiWC1PTEQtQVBJLUtFWSI6ZmFsc2UsImlhdCI6MTc2ODkwNDgzMiwiZXhwIjoxNzY4OTMzODAwfQ.EfgjRi46ust_DSKThA3G52nQVL1SE5lVR9UVU9KolF9Vwzh8hKOPgJZ-LLfGHGwcDWm94JnOY82tYItA1_yeGg",
#     api_key="m1Ak6zez",
# )
#
# # angel.connect()

# from app.core.angel_container import angel
from app.core.angel_container import angel_container


def on_tick(symbol, data):
    asyncio.create_task(manager.fanout(symbol, data))

# def subscribe_symbol(symbol, exchange_type):
#     angel.subscribe(exchange_type, symbol, on_tick)

ANGEL_SYMBOLS = [
    {"exchangeType": 1, "token": "99926009", "name": "Nifty Bank"},
    {"exchangeType": 1, "token": "99926000", "name": "Nifty 50"},
    {"exchangeType": 5, "token": "99920000", "name": "MCXCRUDEX"},
    {"exchangeType": 5, "token": "99920002", "name": "MCXGOLDEX"},
    {"exchangeType": 13, "token": "1", "name": "USDINR"},
    {"exchangeType": 13, "token": "25", "name": "EURINR"},
    {"exchangeType": 13, "token": "26", "name": "GBPINR"},
    {"exchangeType": 13, "token": "27", "name": "JPYINR"},
]

# subscribed = False


def build_token_list():
    token_map = {}
    for s in ANGEL_SYMBOLS:
        token_map.setdefault(s["exchangeType"], []).append(s["token"])

    return [
        {"exchangeType": k, "tokens": v}
        for k, v in token_map.items()
    ]


def unsubscribe_all():
    # global subscribed
    # if not subscribed:
    #     return

    req = {
        "correlationID": "batch_unsub",
        "action": 0,
        "params": {
            "mode": 3,
            "tokenList": build_token_list(),
        },
    }

    angel_container.angel.ws.send(json.dumps(req))
    # subscribed = False
    print("❌ Angel batch unsubscribed")

# def subscribe_all():
#     # global subscribed
#     # if subscribed:
#     #     return
#
#     req = {
#         "correlationID": "batch_sub",
#         "action": 1,
#         "params": {
#             "mode": 3,
#             "tokenList": build_token_list(),
#         },
#     }
#
#     angel.ws.send(json.dumps(req))
#     # subscribed = True
#     print("✅ Angel batch subscribed")
