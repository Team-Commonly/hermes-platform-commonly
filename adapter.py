"""Hermes gateway transport for an installed Commonly agent.

Commonly owns identity and the event queue. Hermes owns the turn. An event is
acknowledged only from BasePlatformAdapter.on_processing_complete, after Hermes
has either delivered its response or deliberately returned no response.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import hashlib
import ipaddress
import logging
import random
import time
from typing import Any
from urllib.parse import quote, urlsplit

import aiohttp

from gateway.config import Platform, PlatformConfig
from gateway.platforms._shared import (
    env_is_connected,
    extra_or_secret,
    get_scoped_secret,
    seed_extra_from_env,
)
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType, ProcessingOutcome

logger = logging.getLogger(__name__)

_PROMPT_EVENTS = frozenset({"chat.mention", "thread.mention", "dm.message", "first_contact"})
# A gateway turn is asynchronous after handle_message() returns. Pull only one
# event until on_processing_complete settles it, so queued turns cannot lose
# their Commonly delivery identity on the same pod.
_MAX_IN_FLIGHT = 1


def _api_origin(value: str) -> str:
    """Require an explicit instance and keep bearer tokens off cleartext networks."""
    value = str(value or "").strip()
    parsed = urlsplit(value)
    if (parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in ("", "/") or not parsed.hostname):
        raise ValueError("COMMONLY_API_URL must be an API origin without credentials, path or query")
    if parsed.scheme == "http":
        try:
            loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = parsed.hostname == "localhost"
        if not loopback:
            raise ValueError("COMMONLY_API_URL must use HTTPS outside loopback")
    elif parsed.scheme != "https":
        raise ValueError("COMMONLY_API_URL must use HTTPS outside loopback")
    try:
        _ = parsed.port  # Refuse malformed ports before constructing the HTTP client.
    except ValueError as error:
        raise ValueError("COMMONLY_API_URL has an invalid port") from error
    return value.rstrip("/")


class CommonlyAdapter(BasePlatformAdapter):
    """Pull addressed events from CAP and post Hermes replies to the same pod."""

    MAX_MESSAGE_LENGTH = 16000

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("commonly"))
        self._token = get_scoped_secret("COMMONLY_AGENT_TOKEN", "") or ""
        self._api_url = extra_or_secret(config.extra, "api_url", "COMMONLY_API_URL") or ""
        self._client: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task | None = None
        self._in_flight: dict[str, dict[str, Any]] = {}
        self._active_by_chat: dict[str, str] = {}
        self._settled: OrderedDict[str, str] = OrderedDict()
        self._chat_types: dict[str, str] = {}
        self._auth_failures = 0

    @property
    def name(self) -> str:
        return "Commonly"

    async def _request(self, method: str, path: str, *, body: dict | None = None,
                       params: dict | None = None) -> dict:
        if self._client is None:
            raise RuntimeError("Commonly adapter is disconnected")
        async with self._client.request(
            method, f"{self._api_url}/api/agents/runtime{path}",
            headers={"Authorization": f"Bearer {self._token}", "x-commonly-client": "hermes-platform"},
            json=body, params=params,
        ) as response:
            if response.status >= 400:
                # Never put the server body in an exception: it can echo a supplied token.
                raise CommonlyHTTPError(response.status)
            return await response.json()

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self._token.startswith("cm_agent_"):
            self._set_fatal_error("config_missing", "COMMONLY_AGENT_TOKEN must be a runtime token", retryable=False)
            return False
        try:
            self._api_url = _api_origin(self._api_url)
        except ValueError as error:
            self._set_fatal_error("bad_api_url", str(error), retryable=False)
            return False
        token_identity = hashlib.sha256(self._token.encode()).hexdigest()[:24]
        if not self._acquire_platform_lock("commonly", token_identity, "Commonly agent identity"):
            return False
        self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        try:
            await self._request("GET", "/installations")
        except (aiohttp.ClientError, asyncio.TimeoutError, CommonlyHTTPError) as error:
            await self._client.close()
            self._client = None
            self._release_platform_lock()
            retryable = not isinstance(error, CommonlyHTTPError) or error.status >= 500
            self._set_fatal_error("connect_failed", f"Commonly API request failed ({type(error).__name__})",
                                  retryable=retryable)
            return False
        self._mark_connected()
        self._poll_task = asyncio.create_task(self._poll_loop())
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            await self._client.close()
            self._client = None
        self._in_flight.clear()
        self._active_by_chat.clear()
        self._chat_types.clear()
        self._release_platform_lock()

    async def _poll_loop(self) -> None:
        delay = 5.0
        while self.is_connected:
            try:
                capacity = _MAX_IN_FLIGHT - len(self._in_flight)
                if capacity > 0:
                    page = await self._request("GET", "/events", params={"limit": min(capacity, 10)})
                    for event in page.get("events", []):
                        await self._receive_event(event)
                elif any(time.monotonic() - state["admitted_at"] > 20 * 60
                         for state in self._in_flight.values()):
                    self._set_fatal_error("turn_stalled", "A Commonly turn did not finish within 20 minutes",
                                          retryable=True)
                    await self._notify_fatal_error()
                    return
                self._auth_failures = 0
                delay = 5.0
            except asyncio.CancelledError:
                raise
            except CommonlyHTTPError as error:
                if error.status in (401, 403):
                    self._auth_failures += 1
                    if self._auth_failures >= 3:
                        self._set_fatal_error("token_rejected", "Commonly runtime token was rejected",
                                              retryable=False)
                        await self._notify_fatal_error()
                        return
                logger.warning("Commonly event poll failed (HTTP %d)", error.status)
                delay = min(delay * 2, 60.0)
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                logger.warning("Commonly event poll failed (%s)", type(error).__name__)
                delay = min(delay * 2, 60.0)
            except Exception as error:
                logger.warning("Commonly event poll failed (%s)", type(error).__name__)
                delay = min(delay * 2, 60.0)
            await asyncio.sleep(delay + random.uniform(0, min(delay / 4, 2.0)))

    async def _receive_event(self, event: dict) -> None:
        event_id = str(event.get("_id") or event.get("id") or "")
        pod_id = str(event.get("podId") or "")
        payload = event.get("payload") or {}
        if not event_id or not pod_id or not isinstance(payload, dict):
            logger.warning("Commonly returned an event without an id, pod or payload")
            return
        if event_id in self._settled:
            await self._ack(event, self._settled[event_id])
            return
        kind = event.get("type")
        if kind == "message.posted":
            # Ambient broadcasts are for agents that opted into the wrapper's claim race.
            # This platform adapter handles direct addressing only.
            await self._ack(event, "no_action")
            return
        if kind not in _PROMPT_EVENTS:
            await self._ack(event, "skipped", "Hermes Commonly platform does not handle this event type")
            return
        content = payload.get("content") or payload.get("prompt") or payload.get("text")
        if not isinstance(content, str) or not content.strip():
            logger.warning("Commonly %s event has no prompt; skipping it", kind)
            await self._ack(event, "skipped", "Addressed event has no text prompt")
            return
        message_id = str(payload.get("messageId") or event_id)
        self._in_flight[event_id] = {
            "event": event, "pod": pod_id, "message": message_id,
            "sent": False, "admitted_at": time.monotonic(),
        }
        source = self.build_source(
            chat_id=pod_id, chat_name=pod_id,
            chat_type="dm" if payload.get("dmPodId") else "group",
            user_id=str(payload.get("userId") or ""),
            user_name=str(payload.get("username") or ""), message_id=message_id,
        )
        self._chat_types[pod_id] = source.chat_type
        inbound = MessageEvent(
            text=content, message_type=MessageType.TEXT, source=source,
            user_id=source.user_id, user_name=source.user_name,
            message_id=message_id, raw_message=event,
            # A Commonly message is untrusted input. Slash text cannot operate Hermes's gateway.
            allow_gateway_control=False, reply_expected=True,
        )
        try:
            await self.handle_message(inbound)
        except Exception:
            self._in_flight.pop(event_id, None)
            raise
        if not inbound._gateway_accepted:
            # Busy sessions may queue this event without setting the admission
            # flag. Keep its delivery identity until a completion hook or the
            # watchdog resolves it; a second poll would race that queued turn.
            logger.warning("Commonly event was not immediately admitted by Hermes")

    async def on_processing_start(self, event: MessageEvent) -> None:
        raw = event.raw_message or {}
        event_id = str(raw.get("_id") or raw.get("id") or "")
        if event_id in self._in_flight:
            self._active_by_chat[event.source.chat_id] = event_id

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        raw = event.raw_message or {}
        event_id = str(raw.get("_id") or raw.get("id") or "")
        state = self._in_flight.pop(event_id, None)
        if state is None:
            return
        if self._active_by_chat.get(state["pod"]) == event_id:
            self._active_by_chat.pop(state["pod"], None)
        if outcome != ProcessingOutcome.SUCCESS:
            return  # Commonly will re-deliver after its lease expires.
        result = "posted" if state["sent"] else "no_action"
        self._settled[event_id] = result
        if len(self._settled) > 1024:
            self._settled.popitem(last=False)
        try:
            await self._ack(raw, result)
        except CommonlyHTTPError as error:
            logger.warning("Commonly event ack failed (HTTP %d)", error.status)
            if error.status in (400, 409):
                self._set_fatal_error("delivery_ack_rejected", "Commonly refused the delivery acknowledgment",
                                      retryable=False)
                await self._notify_fatal_error()
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            logger.warning("Commonly event ack failed (%s)", type(error).__name__)

    async def _ack(self, event: dict, outcome: str, reason: str | None = None) -> None:
        event_id = str(event.get("_id") or event.get("id") or "")
        payload = event.get("payload") or {}
        body: dict[str, Any] = {"result": {"outcome": outcome}}
        if reason:
            body["result"]["reason"] = reason
        if isinstance(payload, dict) and isinstance(payload.get("deliveryId"), str):
            body["deliveryId"] = payload["deliveryId"]
        await self._request("POST", f"/events/{quote(event_id, safe='')}/ack", body=body)

    async def send(self, chat_id: str, content: str, reply_to: str | None = None,
                   metadata: dict | None = None) -> SendResult:
        if not self.is_connected:
            return SendResult(success=False, error="Commonly adapter is disconnected", retryable=True)
        state = self._in_flight.get(self._active_by_chat.get(str(chat_id), ""))
        if content.strip() == "NO_REPLY":
            return SendResult(success=True)
        body: dict[str, Any] = {"content": content}
        anchor = reply_to or (state or {}).get("message")
        if anchor:
            body["replyToMessageId"] = str(anchor)
        try:
            posted = await self._request("POST", f"/pods/{quote(str(chat_id), safe='')}/messages", body=body)
        except CommonlyHTTPError as error:
            return SendResult(success=False, error=f"Commonly HTTP {error.status}",
                              retryable=error.status in (429, 500, 502, 503, 504))
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            return SendResult(success=False, error=type(error).__name__, retryable=True)
        if state is not None:
            state["sent"] = True
        message = posted.get("message") or posted
        return SendResult(success=True, message_id=str(message.get("id") or message.get("_id") or ""))

    async def send_typing(self, chat_id: str, metadata: dict | None = None) -> None:
        # Commonly emits typing when an event is fetched from its queue.
        return None

    async def get_chat_info(self, chat_id: str) -> dict[str, str]:
        return {"name": chat_id, "type": self._chat_types.get(chat_id, "group"), "chat_id": chat_id}


class CommonlyHTTPError(Exception):
    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = status


def _env_enablement() -> dict | None:
    if not get_scoped_secret("COMMONLY_AGENT_TOKEN", ""):
        return None
    return seed_extra_from_env((("COMMONLY_API_URL", "api_url", None),))


def _validate_config(config: PlatformConfig) -> bool:
    return bool(get_scoped_secret("COMMONLY_AGENT_TOKEN", "")
                and extra_or_secret(config.extra, "api_url", "COMMONLY_API_URL"))


def register(ctx) -> None:
    ctx.register_platform(
        name="commonly", label="Commonly", adapter_factory=CommonlyAdapter,
        check_fn=lambda: True, validate_config=_validate_config,
        is_connected=env_is_connected("COMMONLY_AGENT_TOKEN"),
        required_env=["COMMONLY_AGENT_TOKEN", "COMMONLY_API_URL"],
        env_enablement_fn=_env_enablement,
        allowed_users_env="COMMONLY_ALLOWED_USERS",
        allow_all_env="COMMONLY_ALLOW_ALL_USERS",
        max_message_length=CommonlyAdapter.MAX_MESSAGE_LENGTH,
        platform_hint="You are in a Commonly pod. Reply only when your message materially helps the conversation.",
    )
