import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from starlette.responses import JSONResponse

from core.security import (
    AUTH_COOKIE_NAME,
    OptionalAuthMiddleware,
    PUBLIC_PATHS,
    auth_enabled,
    extract_bearer_token,
    is_valid_token,
)


class SecurityTests(unittest.TestCase):
    def test_auth_disabled_without_token(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(auth_enabled())
            self.assertFalse(is_valid_token("anything"))

    def test_validates_env_token(self):
        with patch.dict(os.environ, {"APP_AUTH_TOKEN": "secret"}, clear=True):
            self.assertTrue(auth_enabled())
            self.assertTrue(is_valid_token("secret"))
            self.assertFalse(is_valid_token("wrong"))

    def test_extract_bearer_token(self):
        self.assertEqual("abc", extract_bearer_token("Bearer abc"))
        self.assertEqual("", extract_bearer_token("Basic abc"))

    def test_cookie_name_is_stable(self):
        self.assertEqual("media_scraper_auth", AUTH_COOKIE_NAME)
        self.assertIn("/healthz", PUBLIC_PATHS)

    def test_optional_auth_middleware_allows_token_and_sets_cookie(self):
        middleware = OptionalAuthMiddleware(lambda scope, receive, send: None)

        with patch.dict(os.environ, {"APP_AUTH_TOKEN": "secret"}, clear=True):
            response = asyncio.run(middleware.dispatch(self._request(), self._ok_response))
            self.assertEqual(401, response.status_code)

            response = asyncio.run(
                middleware.dispatch(
                    self._request(query_params={"token": "secret"}),
                    self._ok_response,
                )
            )
            self.assertEqual(200, response.status_code)
            self.assertIn(AUTH_COOKIE_NAME, response.headers.get("set-cookie", ""))

            response = asyncio.run(
                middleware.dispatch(
                    self._request(cookies={AUTH_COOKIE_NAME: "secret"}),
                    self._ok_response,
                )
            )
            self.assertEqual(200, response.status_code)

    def test_health_path_bypasses_auth(self):
        middleware = OptionalAuthMiddleware(lambda scope, receive, send: None)

        with patch.dict(os.environ, {"APP_AUTH_TOKEN": "secret"}, clear=True):
            response = asyncio.run(
                middleware.dispatch(
                    self._request(path="/healthz"),
                    self._ok_response,
                )
            )
            self.assertEqual(200, response.status_code)

    @staticmethod
    def _request(*, path="/", query_params=None, cookies=None, authorization=""):
        return SimpleNamespace(
            headers={"Authorization": authorization} if authorization else {},
            query_params=query_params or {},
            cookies=cookies or {},
            url=SimpleNamespace(path=path),
        )

    @staticmethod
    async def _ok_response(_request):
        return JSONResponse({"ok": True})


if __name__ == "__main__":
    unittest.main()
