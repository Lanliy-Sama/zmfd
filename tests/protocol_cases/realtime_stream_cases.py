import json
import threading
from urllib.request import Request, urlopen

import glm2api as app


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "query weather",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    }
]

TOOL_BLOCK = (
    '<|DSML|tool_calls><|DSML|invoke name="get_weather">'
    '<|DSML|parameter name="city">北京</|DSML|parameter>'
    "</|DSML|invoke></|DSML|tool_calls>"
)


def _sse_event(payload: dict) -> str:
    return "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"


def _delta_event(text: str, phase: str = "answer") -> str:
    return _sse_event({"data": {"delta_content": text, "phase": phase}})


def _done_event() -> str:
    return 'data: {"data":{"done":true,"phase":"answer"},"type":"chat:completion"}\n\n'


class RealtimeStreamCases:
    """Real-time forwarding and incremental tool-call separation."""

    def _collect_filter(self, chunks, tools=None, policy=None):
        stream = app.RealtimeToolCallStream(
            tools or [{"name": "get_weather", "parameters": {"type": "object"}}],
            policy or app.ToolChoice(mode="auto"),
            id_prefix="call_",
        )
        actions = []
        for chunk in chunks:
            actions.extend(stream.feed(chunk))
        actions.extend(stream.finish())
        text = "".join(value for action, value in actions if action == "text")
        calls = [value for action, value in actions if action == "tool_call"]
        return text, calls, stream

    def test_realtime_filter_exhaustive_single_and_double_cuts(self) -> None:
        for cut in range(len(TOOL_BLOCK) + 1):
            text, calls, stream = self._collect_filter([TOOL_BLOCK[:cut], TOOL_BLOCK[cut:]])
            self.assertEqual("", text, f"single cut {cut} leaked text")
            self.assertEqual(1, len(calls), f"single cut {cut}")
            self.assertEqual({"city": "北京"}, calls[0].arguments, f"single cut {cut}")
            self.assertFalse(stream.malformed, f"single cut {cut}")
        for first in range(len(TOOL_BLOCK) + 1):
            for second in range(first, len(TOOL_BLOCK) + 1):
                text, calls, stream = self._collect_filter(
                    [TOOL_BLOCK[:first], TOOL_BLOCK[first:second], TOOL_BLOCK[second:]]
                )
                self.assertEqual("", text, f"double cut {first}/{second} leaked text")
                self.assertEqual(1, len(calls), f"double cut {first}/{second}")
                self.assertEqual({"city": "北京"}, calls[0].arguments, f"double cut {first}/{second}")

    def test_realtime_filter_single_character_chunks(self) -> None:
        stream = app.RealtimeToolCallStream(
            [{"name": "get_weather", "parameters": {"type": "object"}}],
            app.ToolChoice(mode="auto"),
            id_prefix="call_",
        )
        actions = []
        for char in "Hello " + TOOL_BLOCK:
            actions.extend(stream.feed(char))
        actions.extend(stream.finish())
        text = "".join(value for action, value in actions if action == "text")
        calls = [value for action, value in actions if action == "tool_call"]
        self.assertEqual("Hello ", text)
        self.assertEqual(1, len(calls))
        self.assertEqual({"city": "北京"}, calls[0].arguments)
        self.assertFalse(stream.malformed)

    def test_realtime_filter_emits_prose_before_tool_block_finishes(self) -> None:
        stream = app.RealtimeToolCallStream(
            [{"name": "get_weather", "parameters": {"type": "object"}}],
            app.ToolChoice(mode="auto"),
            id_prefix="call_",
        )
        first = stream.feed("Hello ")
        self.assertEqual([("text", "Hello ")], first)
        second = stream.feed(TOOL_BLOCK[:20])
        self.assertEqual([], second)
        actions = stream.feed(TOOL_BLOCK[20:]) + stream.finish()
        self.assertEqual(1, len([action for action, _ in actions if action == "tool_call"]))
        self.assertNotIn("DSML", "".join(value for action, value in actions if action == "text"))

    def test_realtime_filter_ignores_fenced_examples(self) -> None:
        fenced = "```xml\n" + TOOL_BLOCK + "\n```\nAfter"
        text, calls, _stream = self._collect_filter([fenced[:17], fenced[17:]])
        self.assertEqual([], calls)
        self.assertIn("After", text)
        self.assertIn("<|DSML|tool_calls>", text)

    def test_realtime_filter_suppresses_malformed_block(self) -> None:
        malformed = '<tool_calls><invoke name="get_weather"><parameter name="city">'
        text, calls, stream = self._collect_filter([malformed])
        self.assertEqual("", text)
        self.assertEqual([], calls)
        self.assertTrue(stream.malformed)

    def test_realtime_filter_handles_fallback_tool_formats(self) -> None:
        function_call = (
            '<function_call>{"name":"get_weather","arguments":{"city":"A"}}</function_call>'
        )
        stream = app.RealtimeToolCallStream(
            [{"name": "get_weather", "parameters": {"type": "object"}}],
            app.ToolChoice(mode="auto"),
            id_prefix="call_",
        )
        actions = stream.feed(function_call)
        calls = [value for action, value in actions if action == "tool_call"]
        self.assertEqual("", "".join(value for action, value in actions if action == "text"))
        self.assertEqual(1, len(calls))
        self.assertEqual({"city": "A"}, calls[0].arguments)
        self.assertEqual([], stream.finish())

        json_wrapper = (
            '<glm2api_tool_calls>{"tool_calls":'
            '[{"name":"get_weather","arguments":{"city":"B"}}]}</glm2api_tool_calls>'
        )
        stream = app.RealtimeToolCallStream(
            [{"name": "get_weather", "parameters": {"type": "object"}}],
            app.ToolChoice(mode="auto"),
            id_prefix="call_",
        )
        actions = stream.feed(json_wrapper)
        calls = [value for action, value in actions if action == "tool_call"]
        self.assertEqual("", "".join(value for action, value in actions if action == "text"))
        self.assertEqual(1, len(calls))
        self.assertEqual({"city": "B"}, calls[0].arguments)
        self.assertEqual([], stream.finish())

    def test_realtime_filter_handles_bare_json_tool_wrapper(self) -> None:
        wrapper = '{"tool_calls":[{"name":"get_weather","arguments":{"city":"C"}}]}'
        text, calls, _stream = self._collect_filter([wrapper[:13], wrapper[13:]])
        self.assertEqual("", text)
        self.assertEqual(1, len(calls))
        self.assertEqual({"city": "C"}, calls[0].arguments)

        ordinary = '{"answer":"not a tool call"}'
        stream = app.RealtimeToolCallStream(
            [{"name": "get_weather", "parameters": {"type": "object"}}],
            app.ToolChoice(mode="auto"),
            id_prefix="call_",
        )
        immediate = stream.feed(ordinary)
        self.assertEqual(ordinary, "".join(value for action, value in immediate if action == "text"))
        self.assertEqual([], stream.finish())

    def test_realtime_filter_suppresses_standalone_parameter_tag(self) -> None:
        text, calls, stream = self._collect_filter(
            ['before <parameter name="city">A</parameter> after']
        )
        self.assertEqual([], calls)
        self.assertNotIn("parameter", text)
        self.assertTrue(stream.malformed)

    def test_realtime_filter_supports_multiple_calls(self) -> None:
        block = (
            '<|DSML|tool_calls>'
            '<|DSML|invoke name="get_weather"><|DSML|parameter name="city">A</|DSML|parameter></|DSML|invoke>'
            '<|DSML|invoke name="get_weather"><|DSML|parameter name="city">B</|DSML|parameter></|DSML|invoke>'
            '</|DSML|tool_calls>'
        )
        text, calls, _stream = self._collect_filter([block])
        self.assertEqual("", text)
        self.assertEqual(["A", "B"], [call.arguments["city"] for call in calls])

    def test_chat_stream_sends_prose_before_tool_block(self) -> None:
        release = threading.Event()
        observed = threading.Event()
        original = app.stream_zai_completion

        def fake_stream(_state, _prompt, **kwargs):
            context = kwargs.get("context_out")
            if isinstance(context, dict):
                context["chat_id"] = "00000000-0000-0000-0000-0000000000a1"
            yield _delta_event("Hello ")
            if not release.wait(5):
                raise AssertionError("content was not forwarded before the tool block")
            for offset in range(0, len(TOOL_BLOCK), 7):
                yield _delta_event(TOOL_BLOCK[offset : offset + 7])
            yield _done_event()

        app.stream_zai_completion = fake_stream
        try:
            body = {
                "model": "glm-5.5",
                "messages": [{"role": "user", "content": "weather"}],
                "tools": TOOLS,
                "tool_choice": "auto",
                "stream": True,
            }
            request = Request(
                self.base_url + "/v1/chat/completions",
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            chunks = []
            with urlopen(request, timeout=8) as response:
                for raw_line in response:
                    line = raw_line.decode("utf-8", errors="replace")
                    chunks.append(line)
                    if '"content": "Hello "' in line:
                        observed.set()
                        release.set()
                chunks.append(response.read().decode("utf-8", errors="replace"))
        finally:
            app.stream_zai_completion = original
        raw = "".join(chunks)
        self.assertTrue(observed.is_set(), "client did not receive the first prose chunk")
        self.assertNotIn("DSML", raw)
        self.assertIn('"tool_calls"', raw)
        self.assertIn('\\"city\\":\\"北京\\"', raw)
        self.assertIn('"finish_reason": "tool_calls"', raw)

    def test_responses_thinking_is_realtime_with_tools(self) -> None:
        original = app.stream_zai_completion

        def fake_stream(_state, _prompt, **kwargs):
            context = kwargs.get("context_out")
            if isinstance(context, dict):
                context["chat_id"] = "00000000-0000-0000-0000-0000000000a3"
            yield _delta_event("THINK_ONE", "thinking")
            yield _delta_event("-THINK_TWO", "thinking")
            yield _delta_event(TOOL_BLOCK)
            yield _done_event()

        app.stream_zai_completion = fake_stream
        try:
            body = {
                "model": "glm-5.5",
                "input": "weather",
                "tools": [
                    {
                        "type": "function",
                        "name": "get_weather",
                        "parameters": {
                            "type": "object",
                            "properties": {"city": {"type": "string"}},
                            "required": ["city"],
                        },
                    }
                ],
                "tool_choice": "auto",
                "include_thinking": True,
                "stream": True,
                "store": False,
            }
            status, raw = self.request("POST", "/v1/responses", body)
        finally:
            app.stream_zai_completion = original
        self.assertEqual(200, status, raw[:500])
        events = []
        for line in raw.splitlines():
            if not line.startswith("data: "):
                continue
            try:
                events.append(json.loads(line[6:]))
            except ValueError:
                continue
        reasoning_deltas = [
            index
            for index, event in enumerate(events)
            if event.get("type") == "response.reasoning_summary_text.delta"
        ]
        tool_delta = next(
            index
            for index, event in enumerate(events)
            if event.get("type") == "response.function_call_arguments.delta"
        )
        self.assertEqual(2, len(reasoning_deltas), "两个 thinking delta 应各转发一次")
        self.assertLess(reasoning_deltas[-1], tool_delta, "thinking 必须先于工具调用到达")
        self.assertIn("THINK_ONE-THINK_TWO", raw)
        self.assertNotIn("DSML", raw)

    def test_responses_stream_sends_prose_before_tool_block(self) -> None:
        release = threading.Event()
        observed = threading.Event()
        original = app.stream_zai_completion

        def fake_stream(_state, _prompt, **kwargs):
            context = kwargs.get("context_out")
            if isinstance(context, dict):
                context["chat_id"] = "00000000-0000-0000-0000-0000000000a2"
            yield _delta_event("Hello ")
            if not release.wait(5):
                raise AssertionError("content was not forwarded before the tool block")
            for offset in range(0, len(TOOL_BLOCK), 7):
                yield _delta_event(TOOL_BLOCK[offset : offset + 7])
            yield _done_event()

        app.stream_zai_completion = fake_stream
        try:
            body = {
                "model": "glm-5.5",
                "input": "weather",
                "tools": [
                    {
                        "type": "function",
                        "name": "get_weather",
                        "parameters": {
                            "type": "object",
                            "properties": {"city": {"type": "string"}},
                            "required": ["city"],
                        },
                    }
                ],
                "tool_choice": "auto",
                "stream": True,
                "store": False,
            }
            request = Request(
                self.base_url + "/v1/responses",
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            chunks = []
            with urlopen(request, timeout=8) as response:
                for raw_line in response:
                    line = raw_line.decode("utf-8", errors="replace")
                    chunks.append(line)
                    if 'response.output_text.delta' in line and "Hello" in line:
                        observed.set()
                        release.set()
                chunks.append(response.read().decode("utf-8", errors="replace"))
        finally:
            app.stream_zai_completion = original
        raw = "".join(chunks)
        self.assertTrue(observed.is_set(), "client did not receive the first Responses prose chunk")
        self.assertNotIn("DSML", raw)
        self.assertIn("response.function_call_arguments.delta", raw)
        self.assertIn("response.completed", raw)
