import json
import http.cookiejar
import tempfile
import threading
import time
import unittest
from pathlib import Path

from pinnacle_session import (
    AuthenticationChallenge,
    AuthenticationFailed,
    Credentials,
    HttpResponse,
    PinnacleSessionManager,
    SessionExpired,
    SessionStore,
    UrllibTransport,
    load_credentials_from_env,
    parse_auth_result,
)


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, *, headers=None, data=None, timeout=20):
        self.calls.append((method, url, dict(headers or {}), data))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class SlowTransport(FakeTransport):
    def request(self, method, url, *, headers=None, data=None, timeout=20):
        if method == "POST":
            time.sleep(0.02)
        return super().request(method, url, headers=headers, data=data, timeout=timeout)


class PinnacleSessionTests(unittest.TestCase):
    def creds(self):
        return Credentials("demo-user", "demo-password", base_url="https://example.test")

    def response(self, status, body):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        return HttpResponse(status, {"Content-Type": "application/json"}, raw)

    def test_env_loader_requires_credentials_and_keeps_extra_fields(self):
        self.assertIsNone(load_credentials_from_env({}))
        value = load_credentials_from_env({
            "PINNACLE_USERNAME": "u", "PINNACLE_PASSWORD": "p",
            "PINNACLE_AUTH_EXTRA_JSON": '{"fingerprint":"browser-value"}',
        })
        self.assertEqual(value.username, "u")
        self.assertEqual(value.extra["fingerprint"], "browser-value")

    def test_scalar_auth_response_is_not_success(self):
        result = parse_auth_result(self.response(200, b"0"))
        self.assertFalse(result["authenticated"])

    def test_ps3838_scalar_zero_auth_requires_cookie_and_verified_odds(self):
        class CookieTransport(UrllibTransport):
            def __init__(self):
                super().__init__(http.cookiejar.CookieJar())
                self.calls = []

            def request(self, method, url, *, headers=None, data=None, timeout=20):
                self.calls.append(method)
                if method == "POST":
                    self.cookie_jar.set_cookie(http.cookiejar.Cookie(
                        version=0, name="JSESSIONID", value="opaque", port=None,
                        port_specified=False, domain="example.test", domain_specified=True,
                        domain_initial_dot=False, path="/", path_specified=True,
                        secure=False, expires=None, discard=True, comment=None,
                        comment_url=None, rest={}, rfc2109=False,
                    ))
                    return self.response(200, b"0")
                return self.response(200, {"n": [], "l": []})

            @staticmethod
            def response(status, body):
                raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                return HttpResponse(status, {"Content-Type": "application/json"}, raw)

        transport = CookieTransport()
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            status = manager.authenticate()
        self.assertTrue(status["authenticated"])
        self.assertEqual(status["session_mode"], "authenticated_session")
        self.assertIsNotNone(status["last_auth_at"])
        self.assertEqual(transport.calls, ["POST", "GET"])

    def test_otp_response_is_challenge(self):
        result = parse_auth_result(self.response(200, {"status": "OK", "state": "otp"}))
        self.assertTrue(result["challenge"])
        self.assertFalse(result["authenticated"])

    def test_authentication_failure_does_not_expose_password(self):
        transport = FakeTransport([self.response(200, {"status": "ERROR", "code": "BAD_PASSWORD"})])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            with self.assertRaises(AuthenticationFailed):
                manager.authenticate()
            public = repr(manager.status())
            self.assertNotIn("demo-password", public)

    def test_request_authenticates_then_reuses_session(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(200, {"events": []}),
            self.response(200, {"events": []}),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            manager.get("https://example.test/odds")
            manager.get("https://example.test/odds")
        self.assertEqual([call[0] for call in transport.calls], ["POST", "GET", "GET"])

    def test_expired_session_reauthenticates_once_and_retries(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(401, {"error": "unauthorized"}),
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(200, {"events": [1]}),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            result = manager.get("https://example.test/odds")
            self.assertEqual(result.status, 200)
        self.assertEqual([call[0] for call in transport.calls], ["POST", "GET", "POST", "GET"])

    def test_reauth_failure_raises_session_expired(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(401, {}),
            self.response(200, {"status": "ERROR"}),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            with self.assertRaises(AuthenticationFailed):
                manager.get("https://example.test/odds")

    def test_challenge_is_preserved_for_manual_flow(self):
        transport = FakeTransport([self.response(200, {"status": "OK", "state": "otp"})])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            with self.assertRaises(AuthenticationChallenge):
                manager.authenticate()
            self.assertEqual(manager.status()["status"], "等待验证")

    def test_transient_429_has_bounded_retry(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(429, {}), self.response(429, {}), self.response(200, {"events": []}),
        ])
        delays = []
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc",
                                              max_retries=2, sleep_fn=delays.append)
            result = manager.get("https://example.test/odds")
        self.assertEqual(result.status, 200)
        self.assertEqual(len(delays), 2)

    def test_transient_5xx_has_bounded_retry(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(503, {}), self.response(502, {}), self.response(200, {"events": []}),
        ])
        delays = []
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc",
                                              max_retries=2, sleep_fn=delays.append)
            result = manager.get("https://example.test/odds")
        self.assertEqual(result.status, 200)
        self.assertEqual(len(delays), 2)

    def test_network_timeout_has_bounded_retry(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            TimeoutError("timed out"), self.response(200, {"n": [], "l": []}),
        ])
        delays = []
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc",
                                              max_retries=1, sleep_fn=delays.append)
            result = manager.get("https://example.test/odds")
        self.assertEqual(result.status, 200)
        self.assertEqual(len(delays), 1)

    def test_login_lock_prevents_duplicate_authentication(self):
        transport = SlowTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(200, {"events": []}), self.response(200, {"events": []}),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            errors = []
            def worker():
                try:
                    manager.get("https://example.test/odds")
                except Exception as exc:  # pragma: no cover - diagnostic guard
                    errors.append(exc)
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertFalse(errors)
        self.assertEqual(sum(call[0] == "POST" for call in transport.calls), 1)

    def test_status_has_no_sensitive_fields(self):
        transport = FakeTransport([self.response(200, {"status": "OK", "state": "authenticated"})])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            manager.authenticate()
            public = manager.status()
        self.assertNotIn("password", public)
        self.assertNotIn("token", public)
        self.assertNotIn("cookies", public)

    def test_verify_session_requires_odds_shape(self):
        transport = FakeTransport([
            self.response(200, {"status": "OK", "state": "authenticated"}),
            self.response(200, {"html": "<html>login</html>"}),
            self.response(200, {"n": [], "l": []}),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=Path(tmp) / "session.enc")
            manager.authenticate()
            self.assertFalse(manager.verify_session())
            self.assertTrue(manager.verify_session())

    def test_restart_restores_cookie_and_verifies_before_authentication(self):
        class CookieTransport(UrllibTransport):
            def __init__(self, responses):
                super().__init__(http.cookiejar.CookieJar())
                self.responses = list(responses)
                self.calls = []

            def request(self, method, url, *, headers=None, data=None, timeout=20):
                self.calls.append(method)
                return self.responses.pop(0)

        with tempfile.TemporaryDirectory() as tmp:
            session_path = Path(tmp) / "session.enc"
            store = SessionStore(session_path)
            store.save({"cookies": [{"name": "JSESSIONID", "value": "opaque", "domain": "example.test", "path": "/"}]})
            # 首次 request 先做会话验证，再发送实际赔率请求。
            transport = CookieTransport([
                self.response(200, {"n": [], "l": []}),
                self.response(200, {"n": [], "l": []}),
            ])
            manager = PinnacleSessionManager(self.creds(), transport=transport, session_path=session_path)
            self.assertTrue(manager.get("https://example.test/odds").status == 200)
            status = manager.status()
            self.assertEqual(status["session_mode"], "authenticated_session")
            self.assertIsNotNone(status["last_auth_at"])
        self.assertEqual(transport.calls, ["GET", "GET"])


if __name__ == "__main__":
    unittest.main()
