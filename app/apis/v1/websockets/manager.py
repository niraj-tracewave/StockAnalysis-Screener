from collections import defaultdict
from typing import Dict

from fastapi import WebSocket

# class WSManager:
#     def __init__(self):
#         self.connections = {}
#         self.symbol_users = defaultdict(set)
#
#     async def connect(self, user_id, ws: WebSocket):
#         await ws.accept()
#         print("okokokok")
#         self.connections[user_id] = ws
#
#     def disconnect(self, user_id):
#         self.connections.pop(user_id, None)
#         for s in self.symbol_users:
#             self.symbol_users[s].discard(user_id)
#
#     def add_user(self, symbol, user_id):
#         self.symbol_users[symbol].add(user_id)
#
#     async def fanout(self, symbol, data):
#         for uid in self.symbol_users[symbol]:
#             ws = self.connections.get(uid)
#             if ws:
#                 await ws.send_json({"symbol": symbol, "data": data})

class WSManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, user_id, ws: WebSocket):
        await ws.accept()
        self.active_connections[user_id] = ws

    def disconnect(self, user_id):
        self.active_connections.pop(user_id, None)

    def count(self) -> int:
        return len(self.active_connections)

    async def broadcast(self, data: dict):
        disconnected = []
        for user_id, ws in self.active_connections.items():
            try:
                await ws.send_json(data)
            except Exception:
                disconnected.append(user_id)

        for user_id in disconnected:
            self.disconnect(user_id)

manager = WSManager()