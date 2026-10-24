import json
import os
import time
from unittest import mock

import glm2api as app
from protocol_cases.support import QuietProxyHandler, fake_state


class _FakeProcess:
    def __init__(self, stdout: str = "", stderr: str = "") -> None:
        self._stdout = stdout
        self._stderr = stderr
        self.returncode = 0

    def communicate(self, timeout=None):
        return self._stdout, self._stderr

    def poll(self):
        return 0

    def terminate(self):
        pass

    def kill(self):
        pass


FAKE_CAPTCHA = "x" * 240


class _FakePage:
    def __init__(self) -> None:
        self.script = ""
        self.payload = None

    def evaluate(self, script, payload):
        self.script = script
        self.payload = payload
        return FAKE_CAPTCHA


class CaptchaConfigCases:
    def test_captcha_config_matches_live_hosts(self) -> None:
        self.assertEqual(
            {"scene": "xswyjefn", "region": "cn", "prefix": "no8xfe"},
            app.captcha_config_for_host("zchat.z.ai"),
        )
        self.assertEqual("didk33e0", app.captcha_config_for_host("chat.z.ai")["scene"])
        self.assertEqual("cn", app.captcha_config_for_host("chat.z.ai")["region"])
        self.assertEqual("cn", app.current_captcha_config()["region"])
        self.assertEqual("zchat.z.ai", app.urlsplit(app.BASE_URL).hostname)

    def test_captcha_config_env_overrides(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "CAPTCHA_SCENE": "custom-scene",
                "CAPTCHA_REGION": "custom-region",
                "CAPTCHA_PREFIX": "custom-prefix",
            },
        ):
            self.assertEqual(
                {
                    "scene": "custom-scene",
                    "region": "custom-region",
                    "prefix": "custom-prefix",
                },
                app.captcha_config_for_host("zchat.z.ai"),
            )

    def test_happydom_solver_receives_live_captcha_config(self) -> None:
        captured = {}

        def fake_popen(args, **_kwargs):
            captured["args"] = args
            return _FakeProcess('{"ok":true,"captcha":"fake-captcha"}\n')

        with mock.patch.object(app.shutil, "which", return_value="node"), mock.patch.object(
            app, "happydom_captcha_available", return_value=True
        ), mock.patch.object(app.subprocess, "Popen", side_effect=fake_popen):
            value = app.get_happydom_captcha(
                30_000,
                config={"scene": "xswyjefn", "region": "cn", "prefix": "no8xfe"},
            )
        self.assertEqual("fake-captcha", value)
        self.assertEqual(
            [
                "node",
                str(app.HAPPYDOM_CAPTCHA_SCRIPT),
                "--timeout-ms",
                "30000",
                "--scene",
                "xswyjefn",
                "--region",
                "cn",
                "--prefix",
                "no8xfe",
            ],
            captured["args"],
        )

    def test_browser_script_receives_live_captcha_config(self) -> None:
        page = _FakePage()
        self.assertEqual(FAKE_CAPTCHA, app.solve_captcha_on_page(page, 5_000))
        self.assertNotIn("sgp", app.CAPTCHA_EVALUATE_SCRIPT)
        self.assertEqual("xswyjefn", page.payload["sceneId"])
        self.assertEqual("cn", page.payload["region"])
        self.assertEqual("no8xfe", page.payload["prefix"])
        self.assertEqual(5_000, page.payload["timeoutMs"])
        self.assertFalse(page.payload["manual"])

        manual_page = _FakePage()
        self.assertEqual(FAKE_CAPTCHA, app.solve_captcha_on_page(manual_page, 5_000, manual=True))
        self.assertTrue(manual_page.payload["manual"])
        self.assertEqual(5_000, manual_page.payload["timeoutMs"])

    def test_browser_puzzle_is_classified_for_fast_failure(self) -> None:
        class PuzzlePage:
            def evaluate(self, _script, _payload):
                raise RuntimeError("Page.evaluate: Error: puzzle challenge requires manual completion (F001)")

        with self.assertRaises(app.CaptchaPuzzleRequired):
            app.solve_captcha_on_page(PuzzlePage(), 5_000)
        self.assertIn("puzzle challenge requires manual completion", app.CAPTCHA_EVALUATE_SCRIPT)

    def test_short_verify_param_is_rejected_as_puzzle(self) -> None:
        class ShortPage:
            def evaluate(self, _script, _payload):
                return "x" * 48

        with self.assertRaises(app.CaptchaPuzzleRequired):
            app.solve_captcha_on_page(ShortPage(), 5_000)

    def test_resolve_fresh_captcha_breaks_on_puzzle_without_retry(self) -> None:
        state = fake_state()
        # A stale stored value must not mask the puzzle failure.
        state.captcha_verify_param = "stale-value"
        previous_mode = app._CAPTCHA_MODE
        app._CAPTCHA_MODE = "browser"
        app._set_captcha_degraded(0)
        try:
            with mock.patch.object(
                app,
                "get_browser_captcha",
                side_effect=app.CaptchaPuzzleRequired("puzzle challenge requires manual completion (F001)"),
            ) as solver:
                with self.assertRaisesRegex(RuntimeError, "手动验证码"):
                    app.resolve_fresh_captcha(state, "glm-5.5", None, 5_000)
            self.assertEqual(1, solver.call_count)
        finally:
            app._CAPTCHA_MODE = previous_mode
            app._set_captcha_degraded(0)
            with app._MANUAL_CAPTCHA_LOCK:
                app._MANUAL_CAPTCHA_CAPTURES.clear()

    def test_manual_captcha_is_one_shot_and_expires(self) -> None:
        state = fake_state()
        state.captcha_verify_param = "manual-value"
        try:
            app._record_manual_captcha(state)
            self.assertEqual("manual-value", app._take_fresh_manual_captcha(state))
            self.assertEqual("", app._take_fresh_manual_captcha(state))

            app._record_manual_captcha(state)
            with app._MANUAL_CAPTCHA_LOCK:
                app._MANUAL_CAPTCHA_CAPTURES[state.user_id] = (
                    "manual-value",
                    time.time() - app.MANUAL_CAPTCHA_TTL_SECONDS - 1,
                )
            self.assertEqual("", app._take_fresh_manual_captcha(state))
        finally:
            with app._MANUAL_CAPTCHA_LOCK:
                app._MANUAL_CAPTCHA_CAPTURES.clear()

    def test_manual_capture_is_shared_by_same_user_profiles(self) -> None:
        state_a = fake_state()
        state_a.user_id = "shared-user"
        state_a.captcha_verify_param = "shared-value"
        state_b = fake_state()
        state_b.user_id = "shared-user"
        state_b.captcha_verify_param = ""
        try:
            app._record_manual_captcha(state_a)
            self.assertEqual("shared-value", app._take_fresh_manual_captcha(state_b))
        finally:
            with app._MANUAL_CAPTCHA_LOCK:
                app._MANUAL_CAPTCHA_CAPTURES.clear()

    def test_resolve_fresh_captcha_prefers_manual_capture(self) -> None:
        state = fake_state()
        state.captcha_verify_param = "manual-resolve"
        app._record_manual_captcha(state)
        previous_mode = app._CAPTCHA_MODE
        app._CAPTCHA_MODE = "browser"
        try:
            with mock.patch.object(
                app,
                "get_browser_captcha",
                side_effect=AssertionError("browser solver must not run"),
            ):
                value = app.resolve_fresh_captcha(state, "glm-5.5", None, 5_000)
        finally:
            app._CAPTCHA_MODE = previous_mode
            with app._MANUAL_CAPTCHA_LOCK:
                app._MANUAL_CAPTCHA_CAPTURES.clear()
        self.assertEqual("manual-resolve", value)

    def test_on_demand_completion_uses_stored_profile_captcha(self) -> None:
        state = fake_state()
        state.captcha_verify_param = "stored-profile-captcha"
        captured = {}

        class FakeResp:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def __iter__(self):
                return iter([b'data: {"data":{"done":true,"phase":"answer"}}\n\n'])

        def fake_new_chat(_state, _prompt, options=None):
            return "00000000-0000-0000-0000-0000000000f1", "u1"

        def fake_urlopen(request, timeout=None):
            captured["body"] = json.loads(request.data.decode("utf-8"))
            return FakeResp()

        original_new_chat = app.new_chat
        original_urlopen = app.urlopen
        original_stream = app.stream_zai_completion
        app.stream_zai_completion = self.original_stream
        app.new_chat = fake_new_chat
        app.urlopen = fake_urlopen
        try:
            events = app.stream_zai_completion(
                state,
                "hi",
                create_chat=True,
                captcha_verify_param=None,
                fresh_captcha_browser=False,
                options=app.ChatOptions(model="glm-5.5"),
            )
            for _event in events:
                pass
        finally:
            app.stream_zai_completion = original_stream
            app.new_chat = original_new_chat
            app.urlopen = original_urlopen
        self.assertEqual("stored-profile-captcha", captured["body"]["captcha_verify_param"])

    def test_manual_captcha_endpoint_saves_selected_profile(self) -> None:
        state = fake_state()
        profile = app.make_profile(state, "manual-test", "test")
        QuietProxyHandler.profiles = {profile.id: profile}
        QuietProxyHandler.active_profile_id = profile.id
        QuietProxyHandler.state = state
        original = app.get_browser_captcha
        app.get_browser_captcha = lambda *_args, **_kwargs: "manual-endpoint-captcha"
        try:
            with mock.patch.object(app, "playwright_package_available", return_value=True):
                status, raw = self.request(
                    "POST",
                    "/api/auth/captcha/manual",
                    {"profile_id": profile.id, "timeout_sec": 60},
                )
        finally:
            app.get_browser_captcha = original
        data = json.loads(raw)
        self.assertEqual(200, status, raw)
        self.assertTrue(data["ok"])
        self.assertTrue(data["captcha_ok"])
        self.assertEqual(int(app.MANUAL_CAPTCHA_TTL_SECONDS), data["manual_captcha_expires_in"])
        self.assertEqual("manual-endpoint-captcha", profile.state.captcha_verify_param)
        self.assertEqual("manual-endpoint-captcha", app._take_fresh_manual_captcha(profile.state))
