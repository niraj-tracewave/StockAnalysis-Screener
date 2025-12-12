from starlette.responses import JSONResponse


class CustomJSONResponse(JSONResponse):
    def __init__(self, success=True, message=None, data=None, status_code=200):
        content = {
            "meta": {
                "success": success,
                "status_code": status_code,
                "message": message,
            },
            "data": data
        }
        super().__init__(content=content, status_code=status_code)