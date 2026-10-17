"""Cassette misses, JSON logs, and trace redaction."""

from __future__ import annotations

import json
import logging

import pytest

from cdc_lab.cassette import (
    CassetteProvider,
    CompletionRequest,
    default_agent_cassette,
    fill_template,
    request_key,
)
from cdc_lab.clock import SimulatedClock
from cdc_lab.errors import CassetteMiss
from cdc_lab.logging_json import (
    JsonFormatter,
    configure_logging,
    redact_payload,
    redact_text,
)
from cdc_lab.trace import TraceWriter

A20 = "a1B2c3D4e5F6g7H8i9J0"


def test_cassette_miss_is_an_error_and_known_prompts_hit() -> None:
    provider = default_agent_cassette()
    response = provider.complete(
        CompletionRequest(
            model="stub-cassette",
            system="Draft a notification template. Do not send it.",
            prompt="premium_delayed",
            temperature=0.0,
        )
    )
    assert "No enviado" in response.text
    assert response.tokens_in == 0
    body = fill_template(
        response.text,
        {"shipment_id": "s1", "status": "delayed", "rule": "premium_delayed"},
    )
    assert "s1" in body
    with pytest.raises(CassetteMiss):
        provider.complete(
            CompletionRequest(
                model="stub-cassette",
                system="Draft a notification template. Do not send it.",
                prompt="unknown-rule",
                temperature=0.0,
            )
        )
    again = request_key(
        CompletionRequest(
            model="stub-cassette",
            system="Draft a notification template. Do not send it.",
            prompt="premium_delayed",
            temperature=0.0,
        )
    )
    assert again == request_key(
        CompletionRequest(
            model="stub-cassette",
            system="Draft a notification template. Do not send it.",
            prompt="premium_delayed",
            temperature=0.0,
        )
    )


def test_logs_and_traces_redact_secrets_and_direct_identifiers() -> None:
    secret = "sk" + "-" + A20
    assert "REDACTED" in redact_text(f"value {secret}")
    configure_logging("INFO")
    logger = logging.getLogger("cdc_lab.test")
    logger.propagate = True
    # configure_logging attaches to cdc_lab and disables propagation.
    parent = logging.getLogger("cdc_lab")
    parent.propagate = True
    stream = _Stream()
    parent.handlers.clear()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    parent.addHandler(handler)
    parent.setLevel("INFO")
    logger.setLevel("INFO")
    logger.propagate = True
    logger.info(
        "hello %s",
        secret,
        extra={"run_id": "r1", "email": "a@rutasur.example"},
    )
    payload = json.loads(stream.getvalue())
    assert payload["run_id"] == "r1"
    assert payload["level"] == "INFO"
    assert "sk-" not in payload["msg"]
    assert payload["email"] == "[omitted]"
    assert "ts" in payload
    writer = TraceWriter(SimulatedClock(5))
    writer.emit(
        "r1",
        "applied",
        {"email": "cliente-c@rutasur.example", "event_id": "evt-1"},
    )
    line = json.loads(writer.lines[0])
    assert line["ts"] == 5
    assert line["payload"]["email"] == "[omitted]"
    assert line["payload"]["event_id"] == "evt-1"
    nested = redact_payload(
        {
            "note": secret,
            "meta": {"phone": "555"},
            "tags": [secret, {"name": "x"}, 3],
        }
    )
    assert "[REDACTED:openai_key]" in str(nested["note"])
    assert nested["meta"] == {"phone": "[omitted]"}
    assert nested["tags"][2] == 3


class _Stream:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def write(self, text: str) -> int:
        self.parts.append(text)
        return len(text)

    def flush(self) -> None:
        return None

    def getvalue(self) -> str:
        return "".join(self.parts).strip()


def test_empty_cassette() -> None:
    provider = CassetteProvider({})
    with pytest.raises(CassetteMiss):
        provider.complete(CompletionRequest("m", "s", "p"))
