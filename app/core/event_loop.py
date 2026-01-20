# app/core/event_loop.py
import asyncio

class LoopStore:
    def __init__(self):
        self.event_loop: asyncio.AbstractEventLoop | None = None

loop_store = LoopStore()