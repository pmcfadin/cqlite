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
        # Issue #1696 (AH3): the whole `performance` tree (enable_metrics,
        # metrics_interval, enable_profiling, background_tasks.*) was removed —
        # nothing read any of it.
        assert "performance" not in config

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
        # Issue #1696 (AH3): the decorative storage knobs were removed. They had
        # zero production readers, so a config that still names one was silently
        # doing nothing.
        for removed in (
            "max_sstable_size",
            "block_size",
            "enable_bloom_filters",
            "bloom_filter_fp_rate",
            "io_threads",
            "sync_mode",
        ):
            assert removed not in storage
        # The knob the write engine actually reads survives.
        assert "memtable_size_threshold" in storage


class TestRemovedConfigKeysWarn:
    """Issue #1696 (roborev F1): a Python config naming a REMOVED key must LOAD
    and WARN — never be silently ignored, and never hard-fail.

    ``cqlite_core::Config`` is a Rust struct, so an embedder writing Rust who
    still sets a deleted field gets a compile error. Through this bridge there is
    no compile step: serde discards unknown fields, so a pre-change config named
    dead knobs, deserialized successfully, and was ignored in silence. #1696's
    rule — *a removed knob must produce a LOUD signal at the layer where it is
    set* — was therefore false at exactly the layer that cannot get a compile
    error.

    The posture is the same one the CLI's config file uses, deliberately and
    crate-wide: parse-and-ignore PLUS a named warning, never
    ``deny_unknown_fields`` (which would hard-fail a caller whose config predates
    the removal, with no migration path, over keys that never did anything).
    """

    @staticmethod
    def _old_shape_config():
        """A current-shape config with every #1696-removed key put back in.

        Derived from a shipped preset so the surviving half cannot rot, with the
        dead keys written out literally — the shape a saved pre-change config
        still has.
        """
        config = cqlite.performance_optimized()
        config["storage"].update(
            {
                "max_sstable_size": 268435456,
                "block_size": 65536,
                "enable_bloom_filters": True,
                "bloom_filter_fp_rate": 0.01,
                "io_threads": 8,
                "sync_mode": "Normal",
            }
        )
        config["query"].update(
            {
                "plan_cache_size": 1000,
                "enable_optimization": True,
                "parallel": {
                    "enabled": True,
                    "max_threads": 4,
                    "min_parallel_rows": 1000,
                },
            }
        )
        config["performance"] = {
            "enable_metrics": True,
            "metrics_interval": {"secs": 60, "nanos": 0},
            "enable_profiling": False,
            "background_tasks": {
                "enable_stats": True,
                "stats_interval": {"secs": 300, "nanos": 0},
                "enable_cleanup": True,
                "cleanup_interval": {"secs": 3600, "nanos": 0},
            },
        }
        return config

    def test_old_shape_dict_loads_and_warns_by_name(self):
        """An old-shape dict still validates (it LOADS) and names every dead key."""
        config = self._old_shape_config()
        with pytest.warns(UserWarning) as record:
            assert cqlite.validate_config(config) is True

        message = "\n".join(str(w.message) for w in record)
        for removed in (
            "performance",
            "storage.max_sstable_size",
            "storage.block_size",
            "storage.enable_bloom_filters",
            "storage.bloom_filter_fp_rate",
            "storage.io_threads",
            "storage.sync_mode",
            "query.plan_cache_size",
            "query.enable_optimization",
            "query.parallel",
        ):
            assert removed in message, f"the warning must name {removed}: {message}"

    def test_old_shape_json_string_loads_and_warns(self):
        """The JSON-string surface warns too — not only the dict surface."""
        json_config = json.dumps(self._old_shape_config())
        with pytest.warns(UserWarning, match="REMOVED"):
            assert cqlite.validate_config(json_config) is True

    def test_current_shape_config_does_not_warn(self):
        """A clean config is SILENT: the signal must not become per-load noise."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            assert cqlite.validate_config(cqlite.performance_optimized()) is True

    def test_removed_key_warning_is_visible_under_default_filters(self, tmp_path):
        """The warning must reach a user who set NO warning filters at all.

        Issue #1696 roborev r2 F1. The category used to be ``DeprecationWarning``,
        which Python HIDES by default: the stdlib installs
        ``ignore::DeprecationWarning`` with one ``default::...:__main__``
        exception, so an ordinary user importing ``cqlite`` from any module other
        than ``__main__`` saw nothing — the "LOUD signal" was silent at exactly
        the layer this fix exists for.

        Every other test in this class uses ``pytest.warns``, which enables ALL
        warnings and therefore passes for a hidden category too. So this case runs
        a SUBPROCESS with untouched default filters and reads its stderr, and it
        triggers the warning from an IMPORTED module rather than ``__main__``,
        because ``__main__`` is precisely where the hidden category WOULD have
        been shown. Without both properties the test cannot see the defect.
        """
        import os
        import subprocess
        import sys

        probe = tmp_path / "cqlite_removed_key_probe.py"
        probe.write_text(
            "import cqlite\n"
            "\n"
            "def run():\n"
            "    config = cqlite.performance_optimized()\n"
            "    config['storage']['block_size'] = 65536\n"
            "    cqlite.validate_config(config)\n"
        )

        env = dict(os.environ)
        # Inherit the interpreter's import path so the built extension is found,
        # plus the probe module. No warning-filter variable is set: PYTHONWARNINGS
        # is REMOVED so an outer environment cannot make this pass.
        env.pop("PYTHONWARNINGS", None)
        env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), *sys.path])

        result = subprocess.run(
            [sys.executable, "-c", "import cqlite_removed_key_probe as p; p.run()"],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )

        assert result.returncode == 0, (
            f"the probe must LOAD the config, not fail: {result.stderr}"
        )
        assert "UserWarning" in result.stderr, (
            "the removed-key warning must be VISIBLE under Python's default "
            f"filters; stderr was: {result.stderr!r}"
        )
        assert "storage.block_size" in result.stderr, (
            f"the visible warning must NAME the removed key: {result.stderr!r}"
        )

    def test_removed_key_plus_invalid_value_is_rejected_with_no_warning_beside_it(self):
        """A removed key beside an invalid SURVIVING value: reject, and say nothing.

        Issue #1696 roborev r2 F2 — the same defect fixed for the CLI in F3,
        reintroduced on this surface. The warning was raised during PARSING, so a
        document naming a removed key AND carrying an invalid value warned and then
        the public operation REJECTED it: two answers to one call.

        The warning text no longer claims the configuration loads (r5 F1), so what
        is pinned here is the SIGNAL, not the wording: a rejected call reports the
        rejection alone. The "still loads" assertion below is kept as a regression
        guard on that retired wording, so reintroducing it anywhere reds here too.
        """
        import warnings

        config = self._old_shape_config()
        # Invalid in a field that SURVIVED the purge, so the failure is validation
        # and not the removed keys (which never fail a load).
        config["memory"]["max_memory"] = 0

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(ValueError, match="max_memory"):
                cqlite.validate_config(config)

        messages = [str(w.message) for w in caught]
        assert not any("still loads" in m for m in messages), (
            "the retired 'the configuration still loads' assurance must not come "
            f"back — no warning may claim an outcome (r5 F1): {messages}"
        )
        assert not any("REMOVED" in m for m in messages), (
            f"the removed-key warning must not precede the rejection: {messages}"
        )
