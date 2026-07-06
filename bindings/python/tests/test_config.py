"""Tests for cqlite configuration module.

This module tests Issue #289: Implement Configuration Bridge.

Tests verify:
1. StreamingConfig class with sensible defaults
2. Config from dict/JSON/preset all work
3. Invalid config raises ValueError
"""

import json

import pytest

import cqlite


class TestStreamingConfigDefaults:
    """Test StreamingConfig has sensible defaults per issue TDD requirement."""

    def test_streaming_config_defaults(self):
        """StreamingConfig should have buffer_size=1024, chunk_size=10_000."""
        config = cqlite.StreamingConfig()
        assert config.buffer_size == 1024
        assert config.chunk_size == 10_000

    def test_streaming_config_custom_values(self):
        """StreamingConfig should accept custom values."""
        config = cqlite.StreamingConfig(buffer_size=512, chunk_size=5000)
        assert config.buffer_size == 512
        assert config.chunk_size == 5000

    def test_streaming_config_partial_override(self):
        """StreamingConfig should allow partial parameter override."""
        config = cqlite.StreamingConfig(buffer_size=2048)
        assert config.buffer_size == 2048
        assert config.chunk_size == 10_000  # Default preserved

    def test_streaming_config_repr(self):
        """StreamingConfig should have readable repr."""
        config = cqlite.StreamingConfig()
        repr_str = repr(config)
        assert "StreamingConfig" in repr_str
        assert "1024" in repr_str
        assert "10000" in repr_str

    def test_streaming_config_attributes_mutable(self):
        """StreamingConfig attributes should be settable."""
        config = cqlite.StreamingConfig()
        config.buffer_size = 256
        config.chunk_size = 1000
        assert config.buffer_size == 256
        assert config.chunk_size == 1000

    def test_streaming_config_zero_buffer_size_raises(self):
        """StreamingConfig should reject buffer_size=0."""
        with pytest.raises(ValueError, match="buffer_size"):
            cqlite.StreamingConfig(buffer_size=0)

    def test_streaming_config_zero_chunk_size_raises(self):
        """StreamingConfig should reject chunk_size=0."""
        with pytest.raises(ValueError, match="chunk_size"):
            cqlite.StreamingConfig(chunk_size=0)


class TestMemoryOptimizedPreset:
    """Test memory_optimized preset per issue TDD requirement."""

    def test_memory_optimized_preset(self):
        """memory_optimized() should return config with low memory settings."""
        config = cqlite.memory_optimized()
        # Memory should be 256 MB
        assert config["memory"]["max_memory"] == 256 * 1024 * 1024

    def test_memory_optimized_returns_dict(self):
        """memory_optimized() should return a Python dict."""
        config = cqlite.memory_optimized()
        assert isinstance(config, dict)

    def test_memory_optimized_has_storage_config(self):
        """memory_optimized() should include storage configuration."""
        config = cqlite.memory_optimized()
        assert "storage" in config
        assert "compression" in config["storage"]


class TestPerformanceOptimizedPreset:
    """Test performance_optimized preset."""

    def test_performance_optimized_preset(self):
        """performance_optimized() should return config with high memory settings."""
        config = cqlite.performance_optimized()
        # Memory should be 4 GB
        assert config["memory"]["max_memory"] == 4 * 1024 * 1024 * 1024

    def test_performance_optimized_returns_dict(self):
        """performance_optimized() should return a Python dict."""
        config = cqlite.performance_optimized()
        assert isinstance(config, dict)


class TestValidateConfig:
    """Test config validation per issue TDD requirement."""

    def test_valid_config_returns_true(self):
        """validate_config() should return True for valid configs."""
        config = cqlite.memory_optimized()
        assert cqlite.validate_config(config) is True

    def test_invalid_config_raises_valueerror(self):
        """validate_config() should raise ValueError for invalid configs.

        Note: The config must be a complete structure. Using a preset and
        modifying it is the recommended way to create custom configs.
        """
        # Start with a valid preset and modify to make invalid
        invalid_config = cqlite.memory_optimized()
        invalid_config["memory"]["max_memory"] = 0
        with pytest.raises(ValueError, match=r"(?i)max_memory|greater than 0"):
            cqlite.validate_config(invalid_config)

    def test_incomplete_config_raises_valueerror(self):
        """validate_config() should raise ValueError for incomplete configs.

        Partial config dicts are not supported - use a preset and modify.
        """
        # Partial config missing required fields
        partial_config = {"memory": {"max_memory": 128 * 1024 * 1024}}
        with pytest.raises(ValueError, match=r"(?i)missing field|invalid"):
            cqlite.validate_config(partial_config)

    def test_validate_json_string_from_preset(self):
        """validate_config() should accept full JSON config string."""
        # Get a valid config and convert to JSON
        config = cqlite.memory_optimized()
        json_config = json.dumps(config)
        assert cqlite.validate_config(json_config) is True

    def test_validate_preset_string(self):
        """validate_config() should accept preset name string."""
        assert cqlite.validate_config("memory_optimized") is True
        assert cqlite.validate_config("performance_optimized") is True


class TestConfigExports:
    """Test configuration items are properly exported."""

    def test_streaming_config_in_module(self):
        """StreamingConfig should be in cqlite module."""
        assert hasattr(cqlite, "StreamingConfig")

    def test_memory_optimized_in_module(self):
        """memory_optimized should be in cqlite module."""
        assert hasattr(cqlite, "memory_optimized")
        assert callable(cqlite.memory_optimized)

    def test_performance_optimized_in_module(self):
        """performance_optimized should be in cqlite module."""
        assert hasattr(cqlite, "performance_optimized")
        assert callable(cqlite.performance_optimized)

    def test_validate_config_in_module(self):
        """validate_config should be in cqlite module."""
        assert hasattr(cqlite, "validate_config")
        assert callable(cqlite.validate_config)

    def test_config_in_all_exports(self):
        """Config items should be in __all__."""
        assert "StreamingConfig" in cqlite.__all__
        assert "memory_optimized" in cqlite.__all__
        assert "performance_optimized" in cqlite.__all__
        assert "validate_config" in cqlite.__all__


class TestConfigFromDict:
    """Test configuration from Python dict per issue TDD requirement."""

    def test_config_from_dict_structure(self):
        """Config dict should have expected structure."""
        config = cqlite.memory_optimized()

        # Check top-level keys
        assert "storage" in config
        assert "memory" in config
        assert "query" in config
        assert "performance" in config

    def test_config_memory_section(self):
        """Config memory section should have expected fields."""
        config = cqlite.memory_optimized()
        memory = config["memory"]

        assert "max_memory" in memory
        assert "block_cache" in memory
        # Issue #1568 (Epic B/B2): the decorative row_cache/query_cache/allocator
        # knobs were removed; block_cache.max_size is the single real cache knob.
        assert "row_cache" not in memory
        assert "query_cache" not in memory
        assert "allocator" not in memory

    def test_config_storage_section(self):
        """Config storage section should have expected fields."""
        config = cqlite.performance_optimized()
        storage = config["storage"]

        assert "compression" in storage
        assert "compaction" in storage
        assert "enable_bloom_filters" in storage
