"""Fake completion provider. A missing entry is an error, never a network call."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from cdc_lab.errors import CassetteMiss


@dataclass(frozen=True)
class ToolCall:
    name: str
    arguments: Mapping[str, object]


@dataclass(frozen=True)
class CompletionRequest:
    model: str
    system: str
    prompt: str
    temperature: float = 0.0


@dataclass(frozen=True)
class CompletionResponse:
    text: str
    tool_calls: tuple[ToolCall, ...]
    tokens_in: int
    tokens_out: int


class Provider(Protocol):
    def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Return a scripted completion for `request`."""


def request_key(request: CompletionRequest) -> str:
    """Hash of the normalized request. Key order and whitespace do not matter."""
    normalized = {
        "model": request.model,
        "prompt": request.prompt,
        "system": request.system,
        "temperature": request.temperature,
    }
    blob = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class CassetteProvider:
    """Deterministic provider backed by an in-memory recording."""

    def __init__(self, entries: Mapping[str, CompletionResponse]) -> None:
        self._entries = dict(entries)

    def complete(self, request: CompletionRequest) -> CompletionResponse:
        key = request_key(request)
        found = self._entries.get(key)
        if found is None:
            raise CassetteMiss(
                "no cassette entry for "
                f"model {request.model!r} prompt {request.prompt!r}"
            )
        return found


_SYSTEM = "Draft a notification template. Do not send it."
_MODEL = "stub-cassette"


def _scripted(prompt: str, text: str) -> tuple[str, CompletionResponse]:
    request = CompletionRequest(
        model=_MODEL, system=_SYSTEM, prompt=prompt, temperature=0.0
    )
    response = CompletionResponse(text=text, tool_calls=(), tokens_in=0, tokens_out=0)
    return request_key(request), response


def default_agent_cassette() -> CassetteProvider:
    """Two scripted templates. Token counts are zero because nothing was called."""
    entries = dict(
        (
            _scripted(
                "premium_delayed",
                "Borrador pendiente de aprobacion. Envio {{shipment_id}}, "
                "estado {{status}}, regla {{rule}}. No enviado.",
            ),
            _scripted(
                "date_slip",
                "Borrador pendiente de aprobacion. Envio {{shipment_id}} "
                "atrasa {{days}} dias, regla {{rule}}. No enviado.",
            ),
        )
    )
    return CassetteProvider(entries)


def fill_template(template: str, fields: Mapping[str, str]) -> str:
    """Substitute `{{name}}` placeholders. Unknown placeholders stay put."""
    rendered = template
    for name, value in fields.items():
        rendered = rendered.replace("{{" + name + "}}", value)
    return rendered
