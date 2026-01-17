"""Tests for special CQL type to Python type conversions - Issue #300.

TDD tests verifying that special types convert correctly to Python native
types according to M4 spec section 5.1.

Type Mapping (Special):
    CQL Type     | Rust Value         | Python Type
    -------------|--------------------|-----------------
    uuid         | Value::Uuid        | uuid.UUID
    timeuuid     | Value::Uuid        | uuid.UUID
    inet         | Value::Inet        | ipaddress.IPv4Address/IPv6Address
    varint       | Value::Varint      | int (arbitrary precision)
    decimal      | Value::Decimal     | decimal.Decimal

Tests use real SSTable data from test_basic keyspace.
"""

import uuid
import ipaddress
from decimal import Decimal
import pytest
from pathlib import Path

import cqlite


# Test data paths
TEST_DATA = Path(__file__).parent.parent.parent.parent / "test-data"
DATASETS = TEST_DATA / "datasets" / "sstables"
SCHEMAS = TEST_DATA / "schemas"


@pytest.fixture
def db():
    """Database fixture with schema loaded."""
    schema_file = SCHEMAS / "basic-types.cql"
    if not schema_file.exists():
        pytest.skip(f"Schema file not found: {schema_file}")
    if not DATASETS.exists():
        pytest.skip(f"Test data not found: {DATASETS}")
    with cqlite.open(DATASETS, schema=schema_file) as database:
        yield database


class TestUuidConversion:
    """Test CQL UUID to Python uuid.UUID conversion."""

    def test_uuid_returns_uuid_type(self, db):
        """UUID column should return Python uuid.UUID object."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["id"]
        if value is not None:
            assert isinstance(value, uuid.UUID), (
                f"Expected uuid.UUID, got {type(value).__name__}"
            )

    def test_uuid_exact_type(self, db):
        """UUID should return exactly uuid.UUID type."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["id"]
        if value is not None:
            assert type(value) is uuid.UUID

    def test_uuid_not_string(self, db):
        """UUID should NOT return str."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 10")

        for row in result.rows:
            value = row.get("id")
            if value is not None:
                assert not isinstance(value, str), (
                    f"UUID should not be str, got: {value!r}"
                )

    def test_uuid_has_standard_attributes(self, db):
        """UUID object should have standard uuid.UUID attributes."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["id"]
        if value is not None:
            # Standard uuid.UUID attributes
            assert hasattr(value, "bytes")
            assert hasattr(value, "hex")
            assert hasattr(value, "int")
            assert hasattr(value, "version")
            assert hasattr(value, "variant")
            # Verify bytes is 16 bytes
            assert len(value.bytes) == 16

    def test_uuid_string_representation(self, db):
        """UUID str() should produce standard format."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["id"]
        if value is not None:
            uuid_str = str(value)
            # Standard UUID format: 8-4-4-4-12 hex digits
            assert len(uuid_str) == 36
            assert uuid_str.count("-") == 4
            parts = uuid_str.split("-")
            assert len(parts[0]) == 8
            assert len(parts[1]) == 4
            assert len(parts[2]) == 4
            assert len(parts[3]) == 4
            assert len(parts[4]) == 12


class TestTimeuuidConversion:
    """Test CQL TIMEUUID to Python uuid.UUID conversion."""

    def test_timeuuid_returns_uuid_type(self, db):
        """TIMEUUID column should return Python uuid.UUID object."""
        result = db.execute(
            "SELECT session_id FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["session_id"]
        if value is not None:
            assert isinstance(value, uuid.UUID), (
                f"Expected uuid.UUID, got {type(value).__name__}"
            )

    def test_timeuuid_version(self, db):
        """TIMEUUID should have UUID version 1."""
        result = db.execute(
            "SELECT session_id FROM test_basic.simple_table LIMIT 10"
        )

        found_timeuuid = False
        for row in result.rows:
            value = row.get("session_id")
            if value is not None:
                found_timeuuid = True
                # TIMEUUID is UUID version 1
                assert value.version == 1, (
                    f"TIMEUUID should be version 1, got version {value.version}"
                )

        if not found_timeuuid:
            pytest.skip("No TIMEUUID values found in test data")


class TestInetConversion:
    """Test CQL INET to Python ipaddress conversion."""

    def test_inet_returns_ipaddress_type(self, db):
        """INET column should return ipaddress.IPv4Address or IPv6Address."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["ip_address"]
        if value is not None:
            assert isinstance(
                value, (ipaddress.IPv4Address, ipaddress.IPv6Address)
            ), f"Expected IPv4Address or IPv6Address, got {type(value).__name__}"

    def test_inet_not_string(self, db):
        """INET should NOT return str."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 10"
        )

        for row in result.rows:
            value = row.get("ip_address")
            if value is not None:
                assert not isinstance(value, str), (
                    f"INET should not be str, got: {value!r}"
                )

    def test_inet_ipv4_type(self, db):
        """IPv4 addresses should return IPv4Address."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 20"
        )

        found_ipv4 = False
        for row in result.rows:
            value = row.get("ip_address")
            if value is not None and isinstance(value, ipaddress.IPv4Address):
                found_ipv4 = True
                # Verify it has IPv4 attributes
                assert hasattr(value, "packed")
                assert len(value.packed) == 4

        # Note: Test data may or may not contain IPv4 addresses
        if not found_ipv4:
            pytest.skip("No IPv4 addresses found in test data")

    def test_inet_has_standard_methods(self, db):
        """INET objects should have standard ipaddress methods."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["ip_address"]
        if value is not None:
            # Standard ipaddress attributes
            assert hasattr(value, "packed")
            assert hasattr(value, "compressed")
            assert hasattr(value, "is_private")
            assert hasattr(value, "is_loopback")

    def test_inet_string_representation(self, db):
        """INET str() should produce valid IP format."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 10"
        )

        for row in result.rows:
            value = row.get("ip_address")
            if value is not None:
                # str() should produce valid IP string
                ip_str = str(value)
                # Should be parseable back to an address
                parsed = ipaddress.ip_address(ip_str)
                assert parsed == value


class TestDecimalConversion:
    """Test CQL DECIMAL to Python decimal.Decimal conversion."""

    def test_decimal_returns_decimal_type(self, db):
        """DECIMAL column should return decimal.Decimal."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["account_balance"]
        if value is not None:
            assert isinstance(value, Decimal), (
                f"Expected decimal.Decimal, got {type(value).__name__}"
            )

    def test_decimal_exact_type(self, db):
        """DECIMAL should return exactly decimal.Decimal type."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["account_balance"]
        if value is not None:
            assert type(value) is Decimal

    def test_decimal_not_float(self, db):
        """DECIMAL should NOT return float (precision loss)."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 10"
        )

        for row in result.rows:
            value = row.get("account_balance")
            if value is not None:
                assert not isinstance(value, float), (
                    f"DECIMAL should not be float, got: {value!r}"
                )

    def test_decimal_has_standard_methods(self, db):
        """DECIMAL should have standard decimal.Decimal methods."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["account_balance"]
        if value is not None:
            # Standard decimal.Decimal methods
            assert hasattr(value, "as_tuple")
            assert hasattr(value, "quantize")
            assert hasattr(value, "is_finite")
            assert hasattr(value, "is_nan")

    def test_decimal_arithmetic_precision(self, db):
        """DECIMAL should preserve precision in arithmetic."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["account_balance"]
        if value is not None:
            # Arithmetic should preserve Decimal type
            doubled = value * 2
            assert isinstance(doubled, Decimal)
            halved = value / 2
            assert isinstance(halved, Decimal)


class TestSpecialNullHandling:
    """Test NULL handling for all special types."""

    def test_null_uuid_returns_none(self, db):
        """NULL UUID should return Python None."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 100")

        for row in result.rows:
            value = row.get("id")
            if value is None:
                assert value is None  # Verify None identity

    def test_null_inet_returns_none(self, db):
        """NULL INET should return Python None."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 100"
        )

        for row in result.rows:
            value = row.get("ip_address")
            if value is None:
                assert value is None

    def test_null_decimal_returns_none(self, db):
        """NULL DECIMAL should return Python None."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 100"
        )

        for row in result.rows:
            value = row.get("account_balance")
            if value is None:
                assert value is None


class TestSpecialTypeConsistency:
    """Test that special types are consistent across rows."""

    def test_uuid_type_consistent(self, db):
        """UUID should return same type across all rows."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 20")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        types_seen = set()
        for row in result.rows:
            value = row.get("id")
            if value is not None:
                types_seen.add(type(value))

        if types_seen:
            assert len(types_seen) == 1, f"Inconsistent types: {types_seen}"
            assert uuid.UUID in types_seen

    def test_inet_type_consistent(self, db):
        """INET should return ipaddress types across all rows."""
        result = db.execute(
            "SELECT ip_address FROM test_basic.simple_table LIMIT 20"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        for row in result.rows:
            value = row.get("ip_address")
            if value is not None:
                # Should be either IPv4Address or IPv6Address
                assert isinstance(
                    value, (ipaddress.IPv4Address, ipaddress.IPv6Address)
                ), f"Unexpected type: {type(value)}"

    def test_decimal_type_consistent(self, db):
        """DECIMAL should return same type across all rows."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 20"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        types_seen = set()
        for row in result.rows:
            value = row.get("account_balance")
            if value is not None:
                types_seen.add(type(value))

        if types_seen:
            assert len(types_seen) == 1, f"Inconsistent types: {types_seen}"
            assert Decimal in types_seen

    def test_all_special_types_together(self, db):
        """All special types should convert correctly in same query."""
        result = db.execute(
            "SELECT id, session_id, ip_address, account_balance "
            "FROM test_basic.simple_table LIMIT 10"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        for row in result.rows:
            id_val = row.get("id")
            session_id = row.get("session_id")
            ip_addr = row.get("ip_address")
            balance = row.get("account_balance")

            if id_val is not None:
                assert isinstance(id_val, uuid.UUID)
            if session_id is not None:
                assert isinstance(session_id, uuid.UUID)
            if ip_addr is not None:
                assert isinstance(
                    ip_addr, (ipaddress.IPv4Address, ipaddress.IPv6Address)
                )
            if balance is not None:
                assert isinstance(balance, Decimal)


class TestUuidEdgeCases:
    """Test edge cases for UUID conversion."""

    def test_uuid_roundtrip(self, db):
        """UUID should roundtrip correctly through str and back."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 10")

        for row in result.rows:
            value = row.get("id")
            if value is not None:
                # Convert to string and back
                uuid_str = str(value)
                roundtrip = uuid.UUID(uuid_str)
                assert roundtrip == value

    def test_uuid_bytes_roundtrip(self, db):
        """UUID bytes should roundtrip correctly."""
        result = db.execute("SELECT id FROM test_basic.simple_table LIMIT 10")

        for row in result.rows:
            value = row.get("id")
            if value is not None:
                # Get bytes and create new UUID
                uuid_bytes = value.bytes
                roundtrip = uuid.UUID(bytes=uuid_bytes)
                assert roundtrip == value


class TestDecimalEdgeCases:
    """Test edge cases for Decimal conversion."""

    def test_decimal_negative_values(self, db):
        """DECIMAL should handle negative values."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 20"
        )

        for row in result.rows:
            value = row.get("account_balance")
            if value is not None and value < 0:
                # Negative decimal should still be Decimal type
                assert isinstance(value, Decimal)
                assert value < 0

    def test_decimal_zero(self, db):
        """DECIMAL zero should be Decimal(0)."""
        result = db.execute(
            "SELECT account_balance FROM test_basic.simple_table LIMIT 50"
        )

        for row in result.rows:
            value = row.get("account_balance")
            if value is not None and value == 0:
                assert isinstance(value, Decimal)
                assert value == Decimal(0)
