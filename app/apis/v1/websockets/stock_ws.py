# app/apis/v1/websockets/routes.py
from fastapi import APIRouter, WebSocket
from starlette.websockets import WebSocketDisconnect

from app.apis.v1.websockets.manager import manager
from app.core.market_bridge import subscribe_all, unsubscribe_all

router = APIRouter()


@router.websocket("/stock/{user_id}")
async def ws_stock(ws: WebSocket, user_id: str):
    await manager.connect(user_id, ws)
    print("👤 Connected:", user_id)

    if manager.count() == 1:
        subscribe_all()

    try:
        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(user_id)
        print("👋 Disconnected:", user_id)

        if manager.count() == 0:
            unsubscribe_all()
