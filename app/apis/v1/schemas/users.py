from pydantic import BaseModel


class UserLoginSchema(BaseModel):
    mobile_number : str

class VerifyOtpSchema(BaseModel):
    mobile_number : str
    otp : str
    secret : str

class RefreshTokenSchema(BaseModel):
    refresh_token : str