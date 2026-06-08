"""Optional lightweight HTTP/WebSocket access guard."""

import hmac
import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

AUTH_COOKIE_NAME = "media_scraper_auth"
PUBLIC_PATHS = frozenset({"/healthz"})


def get_auth_token() -> str:
    return str(os.environ.get("APP_AUTH_TOKEN") or "").strip()


def auth_enabled() -> bool:
    return bool(get_auth_token())


def is_valid_token(value: str | None) -> bool:
    token = get_auth_token()
    candidate = str(value or "").strip()
    return bool(token and candidate and hmac.compare_digest(candidate, token))


def extract_bearer_token(authorization: str | None) -> str:
    prefix = "Bearer "
    if not authorization or not authorization.startswith(prefix):
        return ""
    return authorization[len(prefix):].strip()


class OptionalAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if getattr(request.url, "path", "") in PUBLIC_PATHS:
            return await call_next(request)
        if not auth_enabled():
            return await call_next(request)

        token = extract_bearer_token(request.headers.get("Authorization"))
        if not token:
            token = request.query_params.get("token", "")
        if not token:
            token = request.cookies.get(AUTH_COOKIE_NAME, "")
        if is_valid_token(token):
            response = await call_next(request)
            if request.query_params.get("token"):
                response.set_cookie(
                    AUTH_COOKIE_NAME,
                    token,
                    httponly=True,
                    samesite="lax",
                    max_age=60 * 60 * 24 * 30,
                )
            return response

        return JSONResponse({"detail": "认证失败"}, status_code=401)
