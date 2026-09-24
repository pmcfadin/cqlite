#!/usr/bin/env python3
"""Derive small CommitLog fixtures and test the structural CRC locator."""

import json
from pathlib import Path
import struct
import sys
import unittest
import zlib


class FixtureFormatError(ValueError):
    """The input does not contain the expected valid Cassandra framing."""


def _require_range(data, start, length, field):
    if start < 0 or length < 0 or start + length > len(data):
        raise FixtureFormatError(
            f"{field} at offset {start} with length {length} exceeds {len(data)} bytes"
        )


def _descriptor(data):
    _require_range(data, 0, 18, "descriptor")
    version, segment_id = struct.unpack_from(">iq", data, 0)
    params_length = struct.unpack_from(">H", data, 12)[0]
    params_start = 14
    params_end = params_start + params_length
    header_end = params_end + 4
    _require_range(data, 0, header_end, "descriptor")
    params = data[params_start:params_end]

    checksum_input = (
        struct.pack(">i", version)
        + struct.pack(">I", segment_id & 0xFFFFFFFF)
        + struct.pack(">I", (segment_id >> 32) & 0xFFFFFFFF)
        + struct.pack(">I", params_length)
        + params
    )
    stored_crc = struct.unpack_from(">I", data, params_end)[0]
    if zlib.crc32(checksum_input) != stored_crc:
        raise FixtureFormatError("descriptor CRC mismatch")
    try:
        decoded_params = json.loads(params.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FixtureFormatError(f"descriptor parameters are not valid JSON: {error}") from error
    if not isinstance(decoded_params, dict):
        raise FixtureFormatError("descriptor parameters must be a JSON object")
    return version, segment_id, params, header_end


def _marker_crc(segment_id, marker_pos):
    checksum_input = (
        struct.pack(">I", segment_id & 0xFFFFFFFF)
        + struct.pack(">I", (segment_id >> 32) & 0xFFFFFFFF)
        + struct.pack(">I", marker_pos)
    )
    return zlib.crc32(checksum_input)


def locate_record_body(data):
    """Return the first fully bounded, CRC-valid record body's offset and size."""
    _, segment_id, _, marker_pos = _descriptor(data)

    while True:
        _require_range(data, marker_pos, 8, "sync marker")
        next_marker = struct.unpack_from(">i", data, marker_pos)[0]
        stored_marker_crc = struct.unpack_from(">I", data, marker_pos + 4)[0]
        if next_marker == 0:
            raise FixtureFormatError("no record body before the clean end marker")
        if next_marker < marker_pos + 8:
            raise FixtureFormatError(
                f"sync marker at {marker_pos} does not point past its marker bytes"
            )
        if stored_marker_crc != _marker_crc(segment_id, marker_pos):
            raise FixtureFormatError(f"sync marker CRC mismatch at offset {marker_pos}")
        if next_marker > len(data):
            raise FixtureFormatError(
                f"sync marker at {marker_pos} points past file end to {next_marker}"
            )

        cursor = marker_pos + 8
        while cursor < next_marker:
            if cursor + 4 > next_marker:
                break  # section padding shorter than a record-size field
            size = struct.unpack_from(">i", data, cursor)[0]
            if size == 0:
                break  # Cassandra's zeroed section padding
            if size < 0:
                raise FixtureFormatError(f"negative record size at offset {cursor}")

            size_crc_pos = cursor + 4
            body_start = cursor + 8
            body_end = body_start + size
            frame_end = body_end + 4
            if frame_end > next_marker or frame_end > len(data):
                raise FixtureFormatError(f"record at offset {cursor} exceeds section or file bounds")

            size_bytes = data[cursor:size_crc_pos]
            stored_size_crc = struct.unpack_from(">I", data, size_crc_pos)[0]
            if zlib.crc32(size_bytes) != stored_size_crc:
                raise FixtureFormatError(f"record size CRC mismatch at offset {cursor}")

            body = data[body_start:body_end]
            stored_body_crc = struct.unpack_from(">I", data, body_end)[0]
            if zlib.crc32(size_bytes + body) != stored_body_crc:
                raise FixtureFormatError(f"record body CRC mismatch at offset {cursor}")
            return body_start, size

        marker_pos = next_marker


def corrupt_record_body(data):
    """Flip a byte in a CRC-validated record body and return bytes plus offset."""
    body_start, body_size = locate_record_body(data)
    flip_offset = body_start + body_size // 2
    corrupted = bytearray(data)
    corrupted[flip_offset] ^= 0xFF

    size_start = body_start - 8
    size_bytes = data[size_start : size_start + 4]
    stored_body_crc = struct.unpack_from(">I", data, body_start + body_size)[0]
    corrupted_body = corrupted[body_start : body_start + body_size]
    if zlib.crc32(size_bytes + corrupted_body) == stored_body_crc:
        raise FixtureFormatError("selected body byte did not invalidate its record CRC")
    return bytes(corrupted), flip_offset


def derive_fixtures(raw_path, output_dir, segment_name):
    raw = Path(raw_path).read_bytes()
    output_dir = Path(output_dir)
    last_nonzero = len(raw)
    while last_nonzero > 0 and raw[last_nonzero - 1] == 0:
        last_nonzero -= 1
    if last_nonzero == 0:
        raise FixtureFormatError("raw segment contains no nonzero bytes")

    clean_end = min(len(raw), last_nonzero + 64)
    clean = raw[:clean_end]
    (output_dir / f"clean-{segment_name}").write_bytes(clean)
    print(f"[trim] raw={len(raw)} clean={len(clean)} (last_nonzero={last_nonzero})")

    tear_at = max(0, last_nonzero - 40)
    (output_dir / f"truncated-{segment_name}").write_bytes(raw[:tear_at])
    print(f"[trunc] tear_at={tear_at}")

    corrupt, flip = corrupt_record_body(clean)
    (output_dir / f"corrupt-crc-{segment_name}").write_bytes(corrupt)
    print(f"[corrupt] flipped record-body byte at {flip}")


def _encode_descriptor(version, segment_id, params):
    prefix = struct.pack(">iqH", version, segment_id, len(params)) + params
    checksum_input = (
        struct.pack(">i", version)
        + struct.pack(">I", segment_id & 0xFFFFFFFF)
        + struct.pack(">I", (segment_id >> 32) & 0xFFFFFFFF)
        + struct.pack(">I", len(params))
        + params
    )
    return prefix + struct.pack(">I", zlib.crc32(checksum_input))


def _shift_descriptor(data):
    """Test helper: extend valid JSON metadata and shift/re-CRC every marker."""
    version, segment_id, params, old_header_end = _descriptor(data)
    shifted_params = params + b" " * 11
    header = _encode_descriptor(version, segment_id, shifted_params)
    delta = len(header) - old_header_end
    shifted = bytearray(header + data[old_header_end:])

    old_pos = old_header_end
    while True:
        _require_range(data, old_pos, 8, "sync marker")
        next_marker = struct.unpack_from(">i", data, old_pos)[0]
        if next_marker == 0:
            break
        if next_marker > len(data) or next_marker < old_pos + 8:
            raise FixtureFormatError("cannot shift an out-of-bounds sync marker")
        if struct.unpack_from(">I", data, old_pos + 4)[0] != _marker_crc(segment_id, old_pos):
            raise FixtureFormatError(f"sync marker CRC mismatch at offset {old_pos}")

        new_pos = old_pos + delta
        new_next_marker = next_marker + delta
        struct.pack_into(">i", shifted, new_pos, new_next_marker)
        struct.pack_into(">I", shifted, new_pos + 4, _marker_crc(segment_id, new_pos))
        old_pos = next_marker
    return bytes(shifted), delta


class CommitLogFixtureDerivationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture_dir = Path(__file__).resolve().parents[1] / "datasets" / "commitlog"
        clean_paths = sorted(fixture_dir.glob("clean-*.log"))
        if len(clean_paths) != 1:
            raise AssertionError(f"expected one committed clean fixture, found {clean_paths}")
        cls.clean = clean_paths[0].read_bytes()

    def assert_corruption_hits_checked_body(self, data):
        body_start, body_size = locate_record_body(data)
        corrupted, flip_offset = corrupt_record_body(data)
        self.assertGreaterEqual(flip_offset, body_start)
        self.assertLess(flip_offset, body_start + body_size)
        self.assertNotEqual(corrupted, data)

        size_start = body_start - 8
        size_bytes = data[size_start : size_start + 4]
        stored_body_crc = struct.unpack_from(">I", data, body_start + body_size)[0]
        self.assertEqual(
            zlib.crc32(size_bytes + data[body_start : body_start + body_size]),
            stored_body_crc,
        )
        self.assertNotEqual(
            zlib.crc32(size_bytes + corrupted[body_start : body_start + body_size]),
            stored_body_crc,
        )
        return flip_offset

    def test_committed_fixture_selects_and_corrupts_a_valid_record_body(self):
        self.assert_corruption_hits_checked_body(self.clean)

    def test_valid_shifted_descriptor_moves_the_selected_body_offset(self):
        original_offset = self.assert_corruption_hits_checked_body(self.clean)
        shifted, delta = _shift_descriptor(self.clean)
        self.assertGreater(delta, 0)
        self.assertEqual(_descriptor(shifted)[3], _descriptor(self.clean)[3] + delta)

        shifted_offset = self.assert_corruption_hits_checked_body(shifted)
        self.assertEqual(shifted_offset, original_offset + delta)


def main(argv):
    if argv == ["--self-test"]:
        unittest.main(argv=[sys.argv[0]], verbosity=2)
        return 0
    if len(argv) == 4 and argv[0] == "derive":
        derive_fixtures(argv[1], argv[2], argv[3])
        return 0
    print(
        f"Usage: {sys.argv[0]} derive <raw-segment> <output-dir> <segment-name>\n"
        f"       {sys.argv[0]} --self-test",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
