from app.core.config import get_settings


# class AngelContainer:
#     angel: Optional[AngelWSClient] = None
#
# angel_container = AngelContainer()


settings = get_settings()
angel = None

def get_angel():
    global angel

    if angel is None:
        # The broker SDK performs network setup during import/initialization, so
        # keep it out of normal HTTP API startup and load it only for a socket user.
        from app.core.angel_auto_login import AngelAutoLogin
        from app.core.angel_ws import AngelWSClient

        print("🚀 Creating Angel instance...")

        try:
            auth = AngelAutoLogin()
            tokens = auth.login()

            angel = AngelWSClient(
                client_id=settings.ANGLE_ONE_CLIENT_ID,
                access_token=tokens["access_token"],
                feed_token=tokens["feed_token"],
                api_key=settings.ANGLE_ONE_API_KEY,
                auto_login=auth
            )

        except Exception as e:
            print("❌ Angel login failed:", e)
            return None

    return angel
