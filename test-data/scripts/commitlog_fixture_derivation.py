#!/usr/bin/env python3
"""Derive small CommitLog fixtures and test the structural CRC locator."""

import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid
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


def iter_valid_frames(data):
    """Yield bounded record offsets after validating each section and record CRC."""
    _, segment_id, _, marker_pos = _descriptor(data)

    while True:
        _require_range(data, marker_pos, 8, "sync marker")
        next_marker = struct.unpack_from(">i", data, marker_pos)[0]
        stored_marker_crc = struct.unpack_from(">I", data, marker_pos + 4)[0]
        if next_marker == 0 and stored_marker_crc == 0:
            return
        if stored_marker_crc != _marker_crc(segment_id, marker_pos):
            raise FixtureFormatError(f"sync marker CRC mismatch at offset {marker_pos}")
        if next_marker < marker_pos + 8:
            raise FixtureFormatError(
                f"sync marker at {marker_pos} does not point past its marker bytes"
            )
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
            yield cursor, body_start, body_end, frame_end, body
            cursor = frame_end

        marker_pos = next_marker


def locate_record_body(data):
    """Return the first fully bounded, CRC-valid record body's offset and size."""
    try:
        _, body_start, body_end, _, _ = next(iter_valid_frames(data))
    except StopIteration as error:
        raise FixtureFormatError("no record body before the clean end marker") from error
    return body_start, body_end - body_start


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


EXPECTED_INSERT_IDS = (1, 2, 3, 4, 5)


def select_truncated_tail(data, ground_truth):
    """Cut within the final known users INSERT after validating its record frame."""
    if ground_truth.get("keyspace") != "commitlog_test" or ground_truth.get("table") != "users":
        raise FixtureFormatError("ground truth is not for commitlog_test.users")
    if ground_truth.get("primary_key") != {
        "partition": [["id", "int"]],
        "clustering": [],
    }:
        raise FixtureFormatError("ground truth must describe the single int id partition key")

    inserts = ground_truth.get("inserts")
    if not isinstance(inserts, list) or tuple(row.get("id") for row in inserts) != EXPECTED_INSERT_IDS:
        raise FixtureFormatError(
            f"expected generated INSERT ids {EXPECTED_INSERT_IDS}, got {inserts!r}"
        )
    try:
        table_id = uuid.UUID(ground_truth["table_id"]).bytes
    except (KeyError, ValueError, AttributeError) as error:
        raise FixtureFormatError("ground truth table_id is not a UUID") from error

    # The fixture generator emits one mutation update per INSERT. In that
    # Cassandra layout, the body begins with update-count 1, then table UUID,
    # the one-byte unsigned-VInt PK length (4), and the big-endian int key.
    target_frames = []
    for frame_start, body_start, body_end, frame_end, body in iter_valid_frames(data):
        if len(body) < 18 or body[0] != 1 or body[1:17] != table_id:
            continue
        if body[17] != 4 or len(body) < 22:
            raise FixtureFormatError(
                f"target INSERT at record offset {frame_start} does not use the expected int PK layout"
            )
        partition_key = struct.unpack_from(">i", body, 18)[0]
        target_frames.append(
            (partition_key, frame_start, body_start, body_end, frame_end)
        )

    found_ids = tuple(frame[0] for frame in target_frames)
    if found_ids != EXPECTED_INSERT_IDS:
        raise FixtureFormatError(
            f"expected one CRC-valid target record for INSERT ids {EXPECTED_INSERT_IDS} "
            f"in order, found {found_ids}"
        )

    target_id, _, body_start, body_end, _ = target_frames[-1]
    cut_at = body_start + (body_end - body_start) // 2
    if not body_start < cut_at < body_end:
        raise FixtureFormatError(f"cannot cut inside target INSERT id={target_id} body")

    complete_prefix = tuple(
        frame[0] for frame in target_frames if frame[4] <= cut_at
    )
    expected_prefix = EXPECTED_INSERT_IDS[:-1]
    if complete_prefix != expected_prefix:
        raise FixtureFormatError(
            f"cut inside INSERT id={target_id} would preserve {complete_prefix}, "
            f"expected {expected_prefix}"
        )
    return cut_at, target_id, complete_prefix


def _ensure_fresh_fixture_output(output_dir):
    existing = sorted(
        path
        for pattern in ("clean-*.log", "truncated-*.log", "corrupt-crc-*.log")
        for path in output_dir.glob(pattern)
    )
    if existing:
        names = ", ".join(path.name for path in existing)
        raise FixtureFormatError(
            f"fixture outputs already exist under {output_dir} ({names}); "
            "refusing to create an ambiguous fixture set"
        )


def derive_fixtures(raw_path, output_dir, segment_name):
    output_dir = Path(output_dir)
    _ensure_fresh_fixture_output(output_dir)
    raw = Path(raw_path).read_bytes()
    last_nonzero = len(raw)
    while last_nonzero > 0 and raw[last_nonzero - 1] == 0:
        last_nonzero -= 1
    if last_nonzero == 0:
        raise FixtureFormatError("raw segment contains no nonzero bytes")

    clean_end = min(len(raw), last_nonzero + 64)
    clean = raw[:clean_end]
    ground_truth_path = output_dir / "commitlog-ground-truth.json"
    try:
        ground_truth = json.loads(ground_truth_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureFormatError(
            f"cannot read fixture ground truth {ground_truth_path}: {error}"
        ) from error
    tear_at, target_id, prefix_ids = select_truncated_tail(clean, ground_truth)
    corrupt, flip = corrupt_record_body(clean)

    (output_dir / f"clean-{segment_name}").write_bytes(clean)
    (output_dir / f"truncated-{segment_name}").write_bytes(clean[:tear_at])
    (output_dir / f"corrupt-crc-{segment_name}").write_bytes(corrupt)
    print(f"[trim] raw={len(raw)} clean={len(clean)} (last_nonzero={last_nonzero})")
    print(f"[trunc] target_id={target_id} cut_at={tear_at} preserved_ids={prefix_ids}")
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
        cls.ground_truth = json.loads(
            (fixture_dir / "commitlog-ground-truth.json").read_text()
        )

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
        cut_at, selected_id, prefix_ids = select_truncated_tail(
            self.clean, self.ground_truth
        )
        self.assertEqual(selected_id, EXPECTED_INSERT_IDS[-1])
        self.assertEqual(prefix_ids, EXPECTED_INSERT_IDS[:-1])
        self.assertLess(cut_at, len(self.clean))

    def test_valid_shifted_descriptor_moves_corruption_and_torn_cut_offsets(self):
        original_offset = self.assert_corruption_hits_checked_body(self.clean)
        original_cut, original_target, original_prefix = select_truncated_tail(
            self.clean, self.ground_truth
        )
        shifted, delta = _shift_descriptor(self.clean)
        self.assertGreater(delta, 0)
        self.assertEqual(_descriptor(shifted)[3], _descriptor(self.clean)[3] + delta)

        shifted_offset = self.assert_corruption_hits_checked_body(shifted)
        self.assertEqual(shifted_offset, original_offset + delta)
        shifted_cut, shifted_target, shifted_prefix = select_truncated_tail(
            shifted, self.ground_truth
        )
        self.assertEqual(shifted_cut, original_cut + delta)
        self.assertEqual(shifted_target, original_target)
        self.assertEqual(shifted_prefix, original_prefix)

    def test_zero_offset_marker_requires_zero_crc(self):
        _, _, _, marker_pos = _descriptor(self.clean)
        while struct.unpack_from(">i", self.clean, marker_pos)[0] != 0:
            marker_pos = struct.unpack_from(">i", self.clean, marker_pos)[0]
        malformed = bytearray(self.clean)
        struct.pack_into(">I", malformed, marker_pos + 4, 1)

        with self.assertRaises(FixtureFormatError):
            list(iter_valid_frames(bytes(malformed)))

    def test_stale_fixture_triplet_is_refused_without_rewriting_ground_truth(self):
        with tempfile.TemporaryDirectory(prefix="cqlite-commitlog-stale-") as name:
            output_dir = Path(name)
            raw_path = output_dir / "raw-input.log"
            raw_path.write_bytes(self.clean)
            ground_truth_path = output_dir / "commitlog-ground-truth.json"
            ground_truth_path.write_bytes(b"ground-truth sentinel")
            stale_paths = [
                output_dir / "clean-old.log",
                output_dir / "truncated-old.log",
                output_dir / "corrupt-crc-old.log",
            ]
            for path in stale_paths:
                path.write_bytes(b"old fixture")

            with self.assertRaisesRegex(FixtureFormatError, "ambiguous fixture set"):
                derive_fixtures(raw_path, output_dir, "new.log")

            self.assertEqual(ground_truth_path.read_bytes(), b"ground-truth sentinel")
            self.assertTrue(all(path.read_bytes() == b"old fixture" for path in stale_paths))
            self.assertFalse((output_dir / "clean-new.log").exists())

    def test_fresh_output_derivation_writes_a_coherent_triplet(self):
        with tempfile.TemporaryDirectory(prefix="cqlite-commitlog-fresh-") as name:
            output_dir = Path(name)
            raw_path = output_dir / "raw-input.log"
            raw_path.write_bytes(self.clean)
            ground_truth_path = output_dir / "commitlog-ground-truth.json"
            ground_truth_bytes = json.dumps(self.ground_truth).encode("utf-8")
            ground_truth_path.write_bytes(ground_truth_bytes)

            derive_fixtures(raw_path, output_dir, "fresh.log")

            cut_at, target_id, prefix_ids = select_truncated_tail(
                self.clean, self.ground_truth
            )
            self.assertEqual(target_id, EXPECTED_INSERT_IDS[-1])
            self.assertEqual(prefix_ids, EXPECTED_INSERT_IDS[:-1])
            self.assertEqual((output_dir / "clean-fresh.log").read_bytes(), self.clean)
            self.assertEqual(
                (output_dir / "truncated-fresh.log").read_bytes(), self.clean[:cut_at]
            )
            self.assertEqual(
                (output_dir / "corrupt-crc-fresh.log").read_bytes(),
                corrupt_record_body(self.clean)[0],
            )
            self.assertEqual(ground_truth_path.read_bytes(), ground_truth_bytes)

    @unittest.skipUnless(os.name == "posix", "fixture generator requires Bash")
    def test_generator_rejects_existing_outputs_and_accepts_fresh_dry_run(self):
        generator = Path(__file__).with_name("generate-commitlog-fixtures.sh")
        with tempfile.TemporaryDirectory(
            prefix="cqlite-commitlog-generator-test-", dir="/tmp"
        ) as name:
            output_dir = Path(name)
            commitlog_dir = output_dir / "commitlog"
            commitlog_dir.mkdir()
            ground_truth_path = commitlog_dir / "commitlog-ground-truth.json"
            ground_truth_path.write_bytes(b"ground-truth sentinel")
            (commitlog_dir / "clean-old.log").write_bytes(b"old clean fixture")

            stale_run = subprocess.run(
                ["bash", str(generator), "--out", str(output_dir), "--dry-run"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(stale_run.returncode, 0)
            self.assertIn("CommitLog output already exists:", stale_run.stderr)
            self.assertNotIn("Starting cassandra:5.0.2", stale_run.stdout)
            self.assertEqual(ground_truth_path.read_bytes(), b"ground-truth sentinel")

        with tempfile.TemporaryDirectory(
            prefix="cqlite-commitlog-generator-fresh-", dir="/tmp"
        ) as name:
            output_dir = Path(name)
            fresh_run = subprocess.run(
                ["bash", str(generator), "--out", str(output_dir), "--dry-run"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                fresh_run.returncode,
                0,
                f"fresh generator dry-run failed\nstdout:\n{fresh_run.stdout}\nstderr:\n{fresh_run.stderr}",
            )
            self.assertFalse((output_dir / "commitlog").exists())


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
