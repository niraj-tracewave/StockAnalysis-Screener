# import websocket
# import json
# import struct
# import threading
#
# class AngelWSClient:
#     def __init__(self, client_id, access_token, api_key):
#         self.client_id = client_id
#         self.access_token = access_token
#         self.api_key = api_key
#
#         self.ws = None
#         self.subscribed_tokens = set()
#         self.token_callbacks = {}  # token -> callback
#
#     def connect(self):
#         url = (
#             f"wss://smartapisocket.angelone.in/smart-stream"
#             f"?client_id={self.client_id}&token={self.access_token}&api_key={self.api_key}"
#         )
#         header = {
#             "Content-Type": "application/json",
#             "Authorization": f"Bearer {self.access_token}",
#             "x-api-key": self.api_key,
#             "x-client-code": self.client_id,
#             "x-feed-token": "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkQ0NjI0NDkiLCJpYXQiOjE3Njg3OTgyMzIsImV4cCI6MTc2ODg4NDYzMn0.47JZnfAiNalp7iUlcU4OTjW7Ot77BGOlinaCEQbsNF-cHDpZaxIdKy6RXOyW5qKh2QWXD8pBC8VuzjsZpS2nKg"
#         }
#         print(header)
#
#         self.ws = websocket.WebSocketApp(
#             url,
#             header=header,
#             on_open=self.on_open,
#             on_message=self.on_message,
#             on_close=self.on_close,
#         )
#
#         threading.Thread(target=self.ws.run_forever, daemon=True).start()
#
#     def on_open(self, ws):
#         print("✅ Angel WebSocket Connected")
#
#     def subscribe(self, exchange_type: int, token: str, callback):
#         if token in self.subscribed_tokens:
#             return  # already subscribed
#
#         sub_req = {
#             "correlationID": "sub",
#             "action": 1,
#             "params": {
#                 "mode": 3,
#                 "tokenList": [
#                     {
#                         "exchangeType": 3,  # 1 = NSE, 2 = BSE
#                         "tokens": ["99919000"]  # Example: RELIANCE
#                     },
#                     {
#                         "exchangeType": 1,  # 1 = NSE, 2 = BSE
#                         "tokens": ["99926000"]  # Example: RELIANCE
#                     }
#                 ]
#             }
#         }
#
#         self.ws.send(json.dumps(sub_req))
#         self.subscribed_tokens.add(token)
#         self.token_callbacks[token] = callback
#
#     def on_message(self, ws, binary_message):
#         token = binary_message[2:27].split(b'\x00')[0].decode()
#
#         try:
#             unpacked = struct.unpack('<6q2d4q', binary_message[27:27+96])
#             data = {
#                 "ltp": unpacked[2] / 100,
#                 "open": unpacked[8] / 100,
#                 "high": unpacked[9] / 100,
#                 "low": unpacked[10] / 100,
#                 "close": unpacked[11] / 100,
#             }
#
#             if token in self.token_callbacks:
#                 self.token_callbacks[token](token, data)
#
#         except Exception as e:
#             print("Parse error:", e)
#
#     def on_close(self, ws, code, reason):
#         print("❌ Angel WS Closed", reason)


# app/core/angel_ws.py
import json
import struct
import threading
import websocket
import asyncio

from app.core.constants import EXCHANGE_TYPE_MAP
from app.core.event_loop import loop_store
from app.apis.v1.websockets.manager import manager
from app.db.redis.redis import redis_client


class AngelWSClient:
    def __init__(self, client_id, access_token, api_key, feed_token, auto_login):
        self.client_id = client_id
        self.access_token = access_token
        self.feed_token = feed_token
        self.api_key = api_key
        self.ws = None
        self.auto_login = auto_login  # AngelAutoLogin instance
        self.reconnecting = False

    def subscribe_all(self):
        from app.core.market_bridge import build_token_list

        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            return

        req = {
            "correlationID": "batch_sub",
            "action": 1,
            "params": {
                "mode": 3,
                "tokenList": build_token_list(),
            },
        }

        self.ws.send(json.dumps(req))
        print("✅ Angel batch subscribed")

    def connect(self):
        try:
            url = (
                "wss://smartapisocket.angelone.in/smart-stream"
                f"?client_id={self.client_id}"
                f"&token={self.feed_token}"
                f"&api_key={self.api_key}"
            )
            headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                            "x-api-key": self.api_key,
                            "x-client-code": self.client_id,
                            "x-feed-token": self.feed_token
                        }

            self.ws = websocket.WebSocketApp(
                url,
                header=[f"{k}: {v}" for k, v in headers.items()],
                on_open=self.on_open,
                on_message=self.on_message,
                on_close=self.on_close,
            )
            threading.Thread(
                target=self.ws.run_forever,
                kwargs={
                    "ping_interval": 25,
                    "ping_timeout": 10,
                },
                daemon=True
            ).start()
        except Exception as e:
            pass

    def reconnect(self):
        if self.reconnecting:
            return

        self.reconnecting = True
        print("🔁 Reconnecting Angel WS (token refresh)...")

        try:
            tokens = self.auto_login.login()  # 🔥 regenerate token

            self.access_token = tokens["access_token"]
            self.feed_token = tokens["feed_token"]

            print("✅ New tokens generated")

            if self.ws:
                try:
                    self.ws.close()
                except:
                    pass

            self.connect()

        except Exception as e:
            print("❌ Reconnect failed:", e)

        finally:
            self.reconnecting = False

    def on_open(self, ws):
        print("✅ Angel WebSocket Connected")
        self.subscribe_all()

    def on_close(self, ws, code, reason):
        print("❌ Angel WebSocket Closed:", reason)
        self.reconnect()

    def on_message(self, ws, binary_message: bytes):
        try:
            token = binary_message[2:27].split(b"\x00")[0].decode()

            unpacked = struct.unpack("<6q2d4q", binary_message[27:123])

            exchange_type_code = binary_message[1]
            exch = EXCHANGE_TYPE_MAP.get(exchange_type_code, "unknown")
            if exch == "cde_fo":
                divisor = 10000000.0  # currency
            else:
                divisor = 100.0  # equity, mcx, indices, etc

            data = {
                "token": token,
                "ltp": unpacked[2] / divisor,
                "open": unpacked[8] / divisor,
                "high": unpacked[9] / divisor,
                "low": unpacked[10] / divisor,
                "close": unpacked[11] / divisor,
            }
            redis_client.set(
                f"last_tick:{token}",
                json.dumps(data)
            )
            if loop_store.event_loop:
                asyncio.run_coroutine_threadsafe(
                    manager.broadcast(data),
                    loop_store.event_loop
                )

        except Exception as e:
            if isinstance(binary_message, str):
                if "token" in binary_message.lower() or "auth" in binary_message.lower():
                    print("⚠️ Token expired detected from server message")
                    self.reconnect()
                    return
