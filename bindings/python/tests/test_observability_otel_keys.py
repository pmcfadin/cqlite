"""``otel_config`` key validation on the Python public surface (issue #1452).

``cqlite-ffi-common::KNOWN_OTEL_KEYS`` is the ONE list of recognised
OpenTelemetry option names shared by both bindings. The Python binding consumes
it as an allowlist: ``cqlite.open(..., otel_config={...})`` raises ``ValueError``
for any key not on that list, so a typo surfaces immediately instead of silently
disabling telemetry.

Node has an enforcing consumer for the same list (``observability.rs`` asserts
its ``OtelOptions`` field names and ``KNOWN_OTEL_KEYS`` are the same set in both
directions). Until this file, Python's half was wired but unasserted — no test
in this suite mentioned ``otel`` at all.

What this asserts, through the **public** surface (``cqlite.open``, the only way
an ``otel_config`` dict reaches the binding):

1. an unrecognised key (the spec's example is a typo'd ``"endpint"``) raises
   ``ValueError``;
2. the message names the offending key;
3. the message's "recognised keys" list is **exactly** the shared crate's
   ``KNOWN_OTEL_KEYS``, read from the committed Rust source — so if the Python
   allowlist silently diverged (a key dropped, added, or renamed on one side)
   this fails rather than passing on a stale hard-coded list;
4. a config using only recognised keys does **not** raise — without which the
   suite could pass by everything raising.

The shared list is read from source because it is not exported to Python and
this issue's diff may not add a test-support export. ``otel_config`` is
validated before any I/O in ``open``, so the negative cases need no corpus; the
happy path opens an empty ``tmp_path`` directory, which is a valid (empty)
database per ``test_database.py::test_open_nonexistent_path_succeeds_empty``.
"""

from __future__ import annotations

import re

import pytest

import cqlite

from conftest import PROJECT_ROOT


# The committed source of truth for the shared list.
_OTEL_KEYS_RS = PROJECT_ROOT / "cqlite-ffi-common" / "src" / "otel_keys.rs"


def _shared_known_otel_keys() -> list[str]:
    """Parse ``KNOWN_OTEL_KEYS`` out of the shared crate's committed source.

    Fail-closed: committed source in a checkout is never legitimately absent or
    unparseable, so a failure here is a real defect, not a reason to skip.
    """
    assert _OTEL_KEYS_RS.is_file(), f"shared key list source missing: {_OTEL_KEYS_RS}"
    text = _OTEL_KEYS_RS.read_text(encoding="utf-8")
    match = re.search(
        r"pub const KNOWN_OTEL_KEYS:\s*&\[&str\]\s*=\s*&\[(?P<body>.*?)\];",
        text,
        re.DOTALL,
    )
    assert match is not None, f"could not locate KNOWN_OTEL_KEYS in {_OTEL_KEYS_RS}"
    keys = re.findall(r'"([^"]+)"', match.group("body"))
    assert keys, f"KNOWN_OTEL_KEYS parsed empty from {_OTEL_KEYS_RS}"
    return keys


def _recognised_keys_from_message(message: str) -> list[str]:
    """The key list the binding printed after ``recognised keys:``."""
    match = re.search(r"recognised keys:\s*(?P<keys>.+)$", message, re.DOTALL)
    assert match is not None, f"message does not list recognised keys: {message!r}"
    return [k.strip() for k in match.group("keys").split(",") if k.strip()]


class TestOtelConfigUnknownKeyRejected:
    """An unrecognised ``otel_config`` key is refused, informatively."""

    def test_typo_key_raises_value_error_naming_the_key(self, tmp_path):
        with pytest.raises(ValueError) as excinfo:
            cqlite.open(tmp_path, otel_config={"endpint": "http://localhost:4317"})

        message = str(excinfo.value)
        assert "endpint" in message, message
        assert "otel_config" in message, message

    def test_message_lists_exactly_the_shared_known_keys(self, tmp_path):
        """The advertised allowlist IS ``KNOWN_OTEL_KEYS`` — both directions.

        A key present in the shared crate but missing from the message (or the
        reverse) means the bindings' allowlist has drifted from the shared list,
        which is the asymmetry issue #1452 exists to prevent.
        """
        shared = _shared_known_otel_keys()

        with pytest.raises(ValueError) as excinfo:
            cqlite.open(tmp_path, otel_config={"endpint": "http://localhost:4317"})

        advertised = _recognised_keys_from_message(str(excinfo.value))

        # A named anchor, so a wholesale parse failure on both sides cannot make
        # this pass by comparing two empty lists.
        assert "endpoint" in shared, shared
        assert set(advertised) == set(shared), (advertised, shared)

    def test_non_string_key_is_rejected(self, tmp_path):
        with pytest.raises(ValueError):
            cqlite.open(tmp_path, otel_config={42: "grpc"})

    def test_non_dict_otel_config_is_rejected(self, tmp_path):
        with pytest.raises(ValueError):
            cqlite.open(tmp_path, otel_config="enabled")


class TestOtelConfigRecognisedKeysAccepted:
    """The happy path: recognised keys open cleanly."""

    def test_recognised_keys_do_not_raise(self, tmp_path):
        db = cqlite.open(
            tmp_path,
            otel_config={
                "enabled": False,
                "endpoint": "http://localhost:4317",
                "protocol": "grpc",
                "service_name": "cqlite-python-test",
                "service_version": "0.0.0-test",
                "sampling_ratio": 0.5,
                "timeout_ms": 1000,
            },
        )
        try:
            assert not db.is_closed
        finally:
            db.close()

    def test_every_shared_key_is_accepted_individually(self, tmp_path):
        """No key on the shared list is rejected by the Python allowlist.

        Complements the message assertion above: that one proves the *advertised*
        list matches, this one proves the *enforced* one does.
        """
        values = {
            "enabled": False,
            "endpoint": "http://localhost:4317",
            "protocol": "grpc",
            "service_name": "cqlite-python-test",
            "service_version": "0.0.0-test",
            "sampling_ratio": 0.5,
            "timeout_ms": 1000,
        }
        for key in _shared_known_otel_keys():
            assert key in values, (
                f"shared key {key!r} has no value in this test; a key was added to "
                "KNOWN_OTEL_KEYS without extending this coverage"
            )
            db = cqlite.open(tmp_path, otel_config={key: values[key]})
            try:
                assert not db.is_closed
            finally:
                db.close()

    def test_empty_otel_config_and_omitted_otel_config_do_not_raise(self, tmp_path):
        for cfg in ({}, None):
            db = cqlite.open(tmp_path, otel_config=cfg)
            db.close()
