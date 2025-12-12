from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.apis.deps import get_db
from app.apis.models.users import User
from app.apis.v1.schemas.users import UserLoginSchema, VerifyOtpSchema, RefreshTokenSchema
from app.core.custom_error_response import CustomValidationError
from app.core.custom_response import CustomJSONResponse
from app.core.jwt_authentication import JWTBearer
from app.core.utils import generate_otp, send_otp, verify_otp
from app.db.postgres.base import BaseDBOperations

jwt_handler = JWTBearer()


class UsersService:

    @staticmethod
    async def login(login_request: UserLoginSchema):
        otp, secret = await generate_otp(login_request.model_dump())
        await send_otp(otp, secret, login_request.model_dump())
        return CustomJSONResponse(
            success=True,
            message="Otp sent successfully.",
            data={"otp": otp, "secret": secret}
        )

    @staticmethod
    async def verify_otp(verify_otp_request: VerifyOtpSchema, db: Session = Depends(get_db)):
        result = await verify_otp(verify_otp_request.model_dump())
        if not result:
            raise CustomValidationError({"error": ["Entered OTP is not valid."]}, 400)

        user_ops = BaseDBOperations(db, User)

        fields = [c.key for c in User.__mapper__.columns if c.key not in ("created_at",)]

        attrs = [getattr(User, f) for f in fields]

        user = await user_ops.retrieve_selected_columns(
            attrs, mobile_number=verify_otp_request.mobile_number
        )

        if not user:
            user = await user_ops.create({
                "mobile_number": verify_otp_request.mobile_number
            })

        access_token_data = jwt_handler.create_access_token({"user_id": 1})
        refresh_token_data = jwt_handler.create_refresh_token({"user_id": 1})

        user_data = jsonable_encoder({k: v for k, v in user.__dict__.items() if k not in ("_sa_instance_state", "created_at")})
        user_data['access_token'] = access_token_data
        user_data['refresh_token'] = refresh_token_data
        return CustomJSONResponse(
                success=True,
                message="Login successfully.",
                data=user_data
            )

    @staticmethod
    async def generate_access_token(refresh_token_request: RefreshTokenSchema):
        access_token_data = jwt_handler.generate_access_token_from_refresh(refresh_token_request.refresh_token)

        return CustomJSONResponse(
            success=True,
            message="Login successfully.",
            data={
                "access_token": access_token_data,
            }
        )
