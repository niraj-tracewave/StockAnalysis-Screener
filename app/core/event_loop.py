# app/core/event_loop.py
import asyncio

event_loop: asyncio.AbstractEventLoop | None = None