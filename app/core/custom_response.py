from starlette.responses import JSONResponse


# class CustomJSONResponse(JSONResponse):
#     def __init__(self, success=True, message=None, data=None, status_code=200):
#         content = {
#             "meta": {
#                 "success": success,
#                 "status_code": status_code,
#                 "message": message,
#             },
#             "data": data
#         }
#         super().__init__(content=content, status_code=status_code)

class CustomJSONResponse:
    @staticmethod
    def custom_response(
            data,
            message="Success",
            status=True,
            status_code=200,
            message_code="SUCCESS",
            platform='WeB@Trac40'
    ):
        content = {
            "meta": {
                "status": status,
                "status_code": status_code,
                "message": message,
                "message_code": message_code
            },
            "data": data
        }
        # if platform not in ["AndRoiD@Trac50", "IoS@Trac60"]:
        #     content = encrypt(content)
        return JSONResponse(
            status_code=status_code,
            content=content
        )