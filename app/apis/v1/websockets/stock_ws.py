import json
from fastapi import APIRouter, WebSocket
from starlette.websockets import WebSocketDisconnect

from app.apis.v1.websockets.manager import manager
from app.core.market_bridge import unsubscribe_all, ANGEL_SYMBOLS
from app.db.redis.redis import redis_client

router = APIRouter()


@router.websocket("/stock/{user_id}")
async def ws_stock(ws: WebSocket, user_id: str):
    await manager.connect(user_id, ws)
    print("👤 Connected:", user_id, manager.count())

    for s in ANGEL_SYMBOLS:
        key = f"last_tick:{s['token']}"
        cached = redis_client.get(key)
        if cached:
            await ws.send_json(json.loads(cached))

    try:
        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(user_id)
        print("👋 Disconnected:", user_id)

        if manager.count() == 0:
            unsubscribe_all()
