from SmartApi import SmartConnect
import pyotp
import os

from app.core.config import get_settings

settings = get_settings()


class AngelAutoLogin:
    def __init__(self):
        self.api_key = settings.ANGLE_ONE_API_KEY
        self.client_code = settings.ANGLE_ONE_CLIENT_CODE
        self.password = settings.ANGLE_ONE_CLIENT_PASSWORD
        self.totp_secret = settings.ANGLE_ONE_TOTP_SECRET

        self.smart = SmartConnect(api_key=self.api_key)

    def login(self):
        totp = pyotp.TOTP(self.totp_secret).now()

        data = self.smart.generateSession(
            self.client_code,
            self.password,
            totp
        )

        if not data["status"]:
            raise Exception(data)

        return {
            "access_token": data["data"]["jwtToken"],
            "feed_token": data["data"]["feedToken"],
        }
