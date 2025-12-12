class CustomValidationError(Exception):
    def __init__(self, validations, status_code=400):
        self.validations = validations
        self.status_code = status_code

    def get_response_data(self):
        response = {
            "meta": {
                "status": False,
                "status_code": self.status_code,
                "message": "Validation error",
                "validations": [self.validations],
            },
            "data": {}
        }

        return response