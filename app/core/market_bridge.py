import asyncio
import json

from app.core.angel_ws import AngelWSClient
from app.apis.v1.websockets.manager import manager

angel = AngelWSClient(
    client_id="6e560bba-4d04-464c-ac06-7e5c9c508792",
    access_token="eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkQ0NjI0NDkiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeUxDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaU9EQmxOalF6TURndE16bGlaUzB6TkdWa0xXRmpOamd0TmpFNU56ZzFORFk1TXpBM0lpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXlJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVElzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOUxDSnRaaUk2ZXlKemRHRjBkWE1pT2lKaFkzUnBkbVVpZlgwc0ltbHpjeUk2SW5SeVlXUmxYMnh2WjJsdVgzTmxjblpwWTJVaUxDSnpkV0lpT2lKRU5EWXlORFE1SWl3aVpYaHdJam94TnpZNE9EZzBOak15TENKdVltWWlPakUzTmpnM09UZ3dOVElzSW1saGRDSTZNVGMyT0RjNU9EQTFNaXdpYW5ScElqb2lPV1k1WWpneFpHWXRaVGM1WkMwME1HWTJMVGhrTjJZdFpEWTVORFV5WkRFNFpUZ3pJaXdpVkc5clpXNGlPaUlpZlEuVHR6VVJXZkN0VzdSOXlFemxVY3VoN3dzTmoySXg0ZDItOWxWSEFVUmMwb1pPVEEzMGh2aE9mUGdPc0JWV0JCTXRrM0VDTmlxdjRLblBaVUptN3lIaDhtdGJZbURBQnpUWktjelJpa19sX01aV0hyYllmVW5US3kzQTJjd0ZoWU04Q0pHSEZhR3k0di1zWTRqU0tsUjRJS3VyUGItbm1jQjUxZ1hqMEQzZ1Q0IiwiQVBJLUtFWSI6Im0xQWs2emV6IiwiWC1PTEQtQVBJLUtFWSI6ZmFsc2UsImlhdCI6MTc2ODc5ODIzMiwiZXhwIjoxNzY4ODQ3NDAwfQ.hrM0S_W1g8tH5ChYOBcT70YIe6phd_azZJXwHROE160sapUgJ5Cn2xyZ6aMKZM8UK2MkIzZLIy_k3Nabe-p8yQ",
    api_key="m1Ak6zez",
)

angel.connect()

def on_tick(symbol, data):
    asyncio.create_task(manager.fanout(symbol, data))

# def subscribe_symbol(symbol, exchange_type):
#     angel.subscribe(exchange_type, symbol, on_tick)

ANGEL_SYMBOLS = [
    {"exchangeType": 3, "token": "99919000"},
    {"exchangeType": 1, "token": "99926000"},
]

subscribed = False


def build_token_list():
    token_map = {}
    for s in ANGEL_SYMBOLS:
        token_map.setdefault(s["exchangeType"], []).append(s["token"])

    return [
        {"exchangeType": k, "tokens": v}
        for k, v in token_map.items()
    ]


def subscribe_all():
    global subscribed
    if subscribed:
        return

    req = {
        "correlationID": "batch_sub",
        "action": 1,
        "params": {
            "mode": 3,
            "tokenList": build_token_list(),
        },
    }

    angel.ws.send(json.dumps(req))
    subscribed = True
    print("✅ Angel batch subscribed")


def unsubscribe_all():
    global subscribed
    if not subscribed:
        return

    req = {
        "correlationID": "batch_unsub",
        "action": 0,
        "params": {
            "mode": 3,
            "tokenList": build_token_list(),
        },
    }

    angel.ws.send(json.dumps(req))
    subscribed = False
    print("❌ Angel batch unsubscribed")

