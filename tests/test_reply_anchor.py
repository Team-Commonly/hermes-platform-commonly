"""Focused CAP reply-anchor contract without a Hermes gateway or a live server."""

import asyncio
from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch


def _load_adapter():
    modules = {name: ModuleType(name) for name in (
        "aiohttp", "gateway", "gateway.config", "gateway.platforms",
        "gateway.platforms._shared", "gateway.platforms.base", "gateway.platforms.event",
    )}
    modules["aiohttp"].ClientError = type("ClientError", (Exception,), {})
    modules["gateway.config"].Platform = lambda name: name
    modules["gateway.config"].PlatformConfig = object

    shared = modules["gateway.platforms._shared"]
    shared.env_is_connected = lambda name: lambda config: True
    shared.extra_or_secret = lambda extra, key, name: "https://api.example.test"
    shared.get_scoped_secret = lambda name, default="": "cm_agent_test" if name == "COMMONLY_AGENT_TOKEN" else default
    shared.seed_extra_from_env = lambda fields: {"api_url": "https://api.example.test"}

    @dataclass
    class SendResult:
        success: bool
        message_id: str = ""
        error: str = ""
        retryable: bool = False

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.is_connected = True

        def build_source(self, **fields):
            return SimpleNamespace(**fields)

        async def handle_message(self, event):
            event._gateway_accepted = True
            await self.on_processing_start(event)
            result = await self.send(event.source.chat_id, "hello", reply_to=event.message_id)
            assert result.success
            await self.on_processing_complete(event, ProcessingOutcome.SUCCESS)

    base = modules["gateway.platforms.base"]
    base.BasePlatformAdapter = BasePlatformAdapter
    base.SendResult = SendResult

    class MessageEvent:
        def __init__(self, **fields):
            self.__dict__.update(fields)

    class ProcessingOutcome:
        SUCCESS = "success"

    event = modules["gateway.platforms.event"]
    event.MessageEvent = MessageEvent
    event.MessageType = SimpleNamespace(TEXT="text")
    event.ProcessingOutcome = ProcessingOutcome

    path = Path(__file__).resolve().parents[1] / "adapter.py"
    spec = spec_from_file_location("commonly_platform_adapter_test", path)
    module = module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


class ReplyAnchorTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        module = _load_adapter()
        self.adapter = module.CommonlyAdapter(SimpleNamespace(extra={}))
        self.requests = []

        async def request(method, path, *, body=None, params=None):
            self.requests.append((method, path, body))
            return {"id": "reply-1"}

        self.adapter._request = request

    async def test_first_contact_posts_without_a_reply_anchor(self):
        await self.adapter._receive_event({
            "_id": "event-1", "podId": "pod-1", "type": "first_contact",
            "payload": {"content": "Welcome", "userId": "user-1", "deliveryId": "delivery-1"},
        })

        self.assertEqual(self.requests[0][2], {"content": "hello"})
        self.assertEqual(self.requests[1][1], "/events/event-1/ack")
        self.assertEqual(self.requests[1][2], {
            "result": {"outcome": "posted"}, "deliveryId": "delivery-1",
        })

    async def test_mention_quotes_only_its_message_id(self):
        await self.adapter._receive_event({
            "_id": "event-2", "podId": "pod-1", "type": "chat.mention",
            "payload": {"content": "@agent hi", "messageId": "message-2", "userId": "user-1"},
        })

        self.assertEqual(self.requests[0][2], {
            "content": "hello", "replyToMessageId": "message-2",
        })
        self.assertEqual(self.requests[1][1], "/events/event-2/ack")
