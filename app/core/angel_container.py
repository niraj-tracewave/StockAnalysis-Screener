from typing import Optional
from app.core.angel_ws import AngelWSClient

class AngelContainer:
    angel: Optional[AngelWSClient] = None

angel_container = AngelContainer()
