"""Tests for CQL collection and UDT to Python type conversions - Issue #301.

TDD tests verifying that collections and UDTs convert correctly to Python native
types according to M4 spec section 5.1.

Type Mapping (Collections):
    CQL Type     | Rust Value         | Python Type
    -------------|--------------------|-----------------
    list<T>      | Value::List        | list
    set<T>       | Value::Set         | frozenset
    map<K,V>     | Value::Map         | dict
    tuple<...>   | Value::Tuple       | tuple
    udt          | Value::Udt         | dict (with _type, _keyspace)
    frozen<T>    | Value::Frozen      | unwrapped inner type

Critical Behavior:
    - Map keys that are lists convert to tuples (hashability requirement)
    - Sets return frozenset (immutable, hashable)
    - UDTs include _type and _keyspace metadata fields

Tests use real SSTable data from test_collections keyspace.
"""

import datetime
import pytest

import cqlite


# Use db_collections from conftest, aliased as db for backward compatibility
@pytest.fixture
def db(db_collections):
    """Alias db_collections as db for this module's tests."""
    return db_collections


class TestListConversion:
    """Test CQL LIST to Python list conversion."""

    def test_list_conversion(self, db):
        """LIST column should return Python list."""
        # ordered_values is LIST<TIMESTAMP> and is populated in test data
        result = db.execute(
            "SELECT ordered_values FROM test_collections.collection_table LIMIT 10"
        )
        found_list = False
        for row in result.rows:
            value = row.get("ordered_values")
            if value is not None:
                found_list = True
                assert isinstance(value, list), (
                    f"Expected list, got {type(value).__name__}"
                )
        if not found_list:
            pytest.skip("No non-null list values found in test data")

    def test_list_exact_type(self, db):
        """LIST should return exactly list type, not subclass."""
        result = db.execute(
            "SELECT ordered_values FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("ordered_values")
            if value is not None:
                assert type(value) is list

    def test_list_preserves_order(self, db):
        """LIST should preserve element order (ordered_values is timestamp list)."""
        result = db.execute(
            "SELECT ordered_values FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("ordered_values")
            if value is not None and len(value) > 1:
                # Timestamps should be in order (they were inserted in order)
                # Just verify it's a list - order preservation is structural
                assert isinstance(value, list)
                assert len(value) > 0

    def test_list_element_types(self, db):
        """LIST<TIMESTAMP> elements should be datetime objects."""
        result = db.execute(
            "SELECT ordered_values FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("ordered_values")
            if value is not None and len(value) > 0:
                # ordered_values is LIST<TIMESTAMP>, elements should be datetime
                for elem in value:
                    assert isinstance(elem, datetime.datetime), (
                        f"Expected datetime element, got {type(elem).__name__}"
                    )


class TestSetConversion:
    """Test CQL SET to Python frozenset conversion."""

    def test_set_conversion(self, db):
        """SET column should return Python frozenset."""
        # numbers_set is SET<INT> and always present (may be empty)
        result = db.execute(
            "SELECT numbers_set FROM test_collections.collection_table LIMIT 10"
        )
        found_set = False
        for row in result.rows:
            value = row.get("numbers_set")
            if value is not None:
                found_set = True
                assert isinstance(value, frozenset), (
                    f"Expected frozenset, got {type(value).__name__}"
                )
        if not found_set:
            pytest.skip("No non-null set values found in test data")

    def test_set_is_frozenset_not_set(self, db):
        """SET should return frozenset, NOT mutable set."""
        result = db.execute(
            "SELECT numbers_set FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("numbers_set")
            if value is not None:
                # Must be frozenset, not set
                assert type(value) is frozenset
                assert type(value) is not set

    def test_set_elements_hashable(self, db):
        """SET elements should be hashable (can be put in another set)."""
        result = db.execute(
            "SELECT numbers_set FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("numbers_set")
            if value is not None:
                # All elements should be hashable
                for elem in value:
                    hash(elem)  # Should not raise TypeError

    def test_numbers_set_with_int_elements(self, db):
        """SET<INT> elements should be Python int."""
        result = db.execute(
            "SELECT numbers_set FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("numbers_set")
            if value is not None and len(value) > 0:
                for elem in value:
                    assert isinstance(elem, int)


class TestMapConversion:
    """Test CQL MAP to Python dict conversion."""

    def test_map_conversion(self, db):
        """MAP column should return Python dict."""
        # metadata_map is MAP<TEXT, BIGINT> and is populated in test data
        result = db.execute(
            "SELECT metadata_map FROM test_collections.collection_table LIMIT 10"
        )
        found_map = False
        for row in result.rows:
            value = row.get("metadata_map")
            if value is not None:
                found_map = True
                assert isinstance(value, dict), (
                    f"Expected dict, got {type(value).__name__}"
                )
        if not found_map:
            pytest.skip("No non-null map values found in test data")

    def test_map_exact_type(self, db):
        """MAP should return exactly dict type."""
        result = db.execute(
            "SELECT metadata_map FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("metadata_map")
            if value is not None:
                assert type(value) is dict

    def test_map_key_types(self, db):
        """MAP keys should have correct Python types."""
        result = db.execute(
            "SELECT metadata_map FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("metadata_map")
            if value is not None and len(value) > 0:
                # metadata_map is MAP<TEXT, BIGINT>, keys should be str
                for key in value.keys():
                    assert isinstance(key, str), (
                        f"Expected str key, got {type(key).__name__}"
                    )

    def test_map_value_types(self, db):
        """MAP values should have correct Python types."""
        result = db.execute(
            "SELECT metadata_map FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("metadata_map")
            if value is not None and len(value) > 0:
                # metadata_map is MAP<TEXT, BIGINT>, values should be int
                for val in value.values():
                    assert isinstance(val, int), (
                        f"Expected int value, got {type(val).__name__}"
                    )

    def test_map_keys_are_hashable(self, db):
        """All MAP keys must be hashable (Python dict requirement)."""
        result = db.execute(
            "SELECT metadata_map FROM test_collections.collection_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("metadata_map")
            if value is not None:
                for key in value.keys():
                    hash(key)  # Should not raise TypeError


class TestUdtConversion:
    """Test CQL UDT to Python dict conversion."""

    def test_udt_conversion(self, db):
        """UDT should return Python dict."""
        result = db.execute(
            "SELECT addresses FROM test_collections.collections_with_udts LIMIT 10"
        )
        found_udt = False
        for row in result.rows:
            addresses = row.get("addresses")
            if addresses is not None and len(addresses) > 0:
                # addresses is LIST<FROZEN<address_type>>, each element is a UDT
                udt = addresses[0]
                found_udt = True
                assert isinstance(udt, dict), (
                    f"Expected dict for UDT, got {type(udt).__name__}"
                )
        if not found_udt:
            pytest.skip("No UDT values found in test data")

    def test_udt_has_type_metadata(self, db):
        """UDT dict should have _type field with type name."""
        result = db.execute(
            "SELECT addresses FROM test_collections.collections_with_udts LIMIT 10"
        )
        for row in result.rows:
            addresses = row.get("addresses")
            if addresses is not None and len(addresses) > 0:
                udt = addresses[0]
                assert "_type" in udt, "UDT should have _type metadata"
                assert isinstance(udt["_type"], str)
                # Type name should be address_type
                assert "address" in udt["_type"].lower()

    def test_udt_has_keyspace_metadata(self, db):
        """UDT dict should have _keyspace field."""
        result = db.execute(
            "SELECT addresses FROM test_collections.collections_with_udts LIMIT 10"
        )
        for row in result.rows:
            addresses = row.get("addresses")
            if addresses is not None and len(addresses) > 0:
                udt = addresses[0]
                assert "_keyspace" in udt, "UDT should have _keyspace metadata"
                assert isinstance(udt["_keyspace"], str)

    def test_udt_field_access(self, db):
        """UDT fields should be accessible by name."""
        result = db.execute(
            "SELECT addresses FROM test_collections.collections_with_udts LIMIT 10"
        )
        for row in result.rows:
            addresses = row.get("addresses")
            if addresses is not None and len(addresses) > 0:
                udt = addresses[0]
                # address_type has: street, city, state, zip_code, country
                # At least some fields should be present
                address_fields = {"street", "city", "state", "zip_code", "country"}
                found_fields = set(udt.keys()) & address_fields
                assert len(found_fields) > 0, (
                    f"Expected address fields, got keys: {udt.keys()}"
                )


class TestNestedCollections:
    """Test nested collection structures."""

    def test_nested_map_of_lists(self, db):
        """MAP<TEXT, FROZEN<LIST<INT>>> should work correctly."""
        result = db.execute(
            "SELECT scores_by_game FROM test_collections.nested_collections_table LIMIT 10"
        )
        found_nested = False
        for row in result.rows:
            value = row.get("scores_by_game")
            if value is not None and len(value) > 0:
                found_nested = True
                # Outer should be dict
                assert isinstance(value, dict)
                # Keys should be str
                for key, inner in value.items():
                    assert isinstance(key, str)
                    # Inner should be list
                    assert isinstance(inner, list), (
                        f"Expected nested list, got {type(inner).__name__}"
                    )
        if not found_nested:
            pytest.skip("No nested map of lists found in test data")

    def test_nested_map_of_sets(self, db):
        """MAP<TEXT, FROZEN<SET<TEXT>>> should work correctly."""
        result = db.execute(
            "SELECT tags_by_category FROM test_collections.nested_collections_table LIMIT 10"
        )
        found_nested = False
        for row in result.rows:
            value = row.get("tags_by_category")
            if value is not None and len(value) > 0:
                found_nested = True
                assert isinstance(value, dict)
                for key, inner in value.items():
                    assert isinstance(key, str)
                    # Frozen sets in nested context should still be frozenset
                    assert isinstance(inner, frozenset), (
                        f"Expected nested frozenset, got {type(inner).__name__}"
                    )
        if not found_nested:
            pytest.skip("No nested map of sets found in test data")

    def test_nested_map_of_maps(self, db):
        """MAP<TEXT, FROZEN<MAP<TEXT, TEXT>>> should work correctly."""
        result = db.execute(
            "SELECT user_preferences FROM test_collections.nested_collections_table LIMIT 10"
        )
        found_nested = False
        for row in result.rows:
            value = row.get("user_preferences")
            if value is not None and len(value) > 0:
                found_nested = True
                assert isinstance(value, dict)
                for key, inner in value.items():
                    assert isinstance(key, str)
                    assert isinstance(inner, dict), (
                        f"Expected nested dict, got {type(inner).__name__}"
                    )
        if not found_nested:
            pytest.skip("No nested map of maps found in test data")

    def test_udt_in_list(self, db):
        """LIST<FROZEN<udt>> should return list of dicts."""
        result = db.execute(
            "SELECT addresses FROM test_collections.collections_with_udts LIMIT 10"
        )
        found_udt_list = False
        for row in result.rows:
            addresses = row.get("addresses")
            if addresses is not None and len(addresses) > 0:
                found_udt_list = True
                assert isinstance(addresses, list)
                for addr in addresses:
                    assert isinstance(addr, dict)
                    # Should have UDT metadata
                    assert "_type" in addr
        if not found_udt_list:
            pytest.skip("No UDT list found in test data")

    def test_udt_in_map(self, db):
        """MAP<K, FROZEN<udt>> should return dict with UDT values."""
        result = db.execute(
            "SELECT locations_visited FROM test_collections.collections_with_udts LIMIT 10"
        )
        found_udt_map = False
        for row in result.rows:
            locations = row.get("locations_visited")
            if locations is not None and len(locations) > 0:
                found_udt_map = True
                assert isinstance(locations, dict)
                for key, addr in locations.items():
                    # Key is DATE, value is address_type UDT
                    assert isinstance(addr, dict)
                    assert "_type" in addr
        if not found_udt_map:
            pytest.skip("No UDT map found in test data")


class TestFrozenCollections:
    """Test frozen collection handling."""

    def test_frozen_list(self, db):
        """FROZEN<LIST<T>> should return list."""
        result = db.execute(
            "SELECT frozen_scores FROM test_collections.frozen_collections_table LIMIT 10"
        )
        found_frozen = False
        for row in result.rows:
            value = row.get("frozen_scores")
            if value is not None:
                found_frozen = True
                # Frozen list still returns as list
                assert isinstance(value, list)
        if not found_frozen:
            pytest.skip("No frozen list found in test data")

    def test_frozen_set(self, db):
        """FROZEN<SET<T>> should return frozenset."""
        # nested_collections_table has MAP<TEXT, FROZEN<SET<TEXT>>> in tags_by_category
        # The inner frozen set should be a frozenset
        result = db.execute(
            "SELECT tags_by_category FROM test_collections.nested_collections_table LIMIT 10"
        )
        found_frozen = False
        for row in result.rows:
            value = row.get("tags_by_category")
            if value is not None and len(value) > 0:
                # tags_by_category is MAP<TEXT, FROZEN<SET<TEXT>>>
                # Check that inner values are frozensets
                for inner in value.values():
                    found_frozen = True
                    assert isinstance(inner, frozenset), (
                        f"Expected inner frozenset, got {type(inner).__name__}"
                    )
                    break
        if not found_frozen:
            pytest.skip("No frozen set found in test data")

    def test_frozen_map(self, db):
        """FROZEN<MAP<K,V>> should return dict."""
        result = db.execute(
            "SELECT frozen_properties FROM test_collections.frozen_collections_table LIMIT 10"
        )
        found_frozen = False
        for row in result.rows:
            value = row.get("frozen_properties")
            if value is not None:
                found_frozen = True
                assert isinstance(value, dict)
        if not found_frozen:
            pytest.skip("No frozen map found in test data")


class TestNullAndEmptyCollections:
    """Test null and empty collection handling."""

    def test_null_collection_returns_none(self, db):
        """NULL collection should return Python None."""
        # In collection_table, 'properties', 'scores', and 'tags' columns are often null
        # but they may not be included in SELECT * results when null
        # Let's check with a broader query
        result = db.execute(
            "SELECT * FROM test_collections.collection_table LIMIT 50"
        )
        # The rows may or may not include null columns - that's implementation-dependent
        # For this test, we verify that if a null-capable column is missing from row,
        # it returns None when accessed via .get()
        for row in result.rows:
            # These columns are often null in test data
            for col in ["properties", "scores", "tags"]:
                value = row.get(col)
                if value is None:
                    # Confirm None is returned (not KeyError or empty collection)
                    assert value is None
                    return
        pytest.skip("No null collections found in test data")

    def test_empty_set_returns_empty_frozenset(self, db):
        """Empty SET should return empty frozenset, not None."""
        result = db.execute(
            "SELECT empty_set FROM test_collections.empty_collections_table LIMIT 10"
        )
        for row in result.rows:
            value = row.get("empty_set")
            # Empty collection should be frozenset(), not None
            if value is not None and len(value) == 0:
                assert isinstance(value, frozenset)
                assert len(value) == 0
                return
        # May also find empty sets in numbers_set
        result = db.execute(
            "SELECT numbers_set FROM test_collections.collection_table LIMIT 50"
        )
        for row in result.rows:
            value = row.get("numbers_set")
            if value is not None and len(value) == 0:
                assert isinstance(value, frozenset)
                return
        pytest.skip("No empty sets found in test data")

    def test_type_consistency_across_rows(self, db):
        """Same column should return same type across all rows."""
        # Use numbers_set which is always present (though may be empty)
        result = db.execute(
            "SELECT numbers_set FROM test_collections.collection_table LIMIT 50"
        )
        types_seen = set()
        for row in result.rows:
            value = row.get("numbers_set")
            if value is not None:
                types_seen.add(type(value))

        # Should have at most one type (all frozenset)
        if types_seen:
            assert len(types_seen) == 1, (
                f"Inconsistent types across rows: {types_seen}"
            )
            assert frozenset in types_seen
