"""Basic tests for cqlite Python bindings."""

import cqlite


def test_version():
    """Test version string is accessible and follows semver format."""
    version = cqlite.version()
    assert isinstance(version, str)
    assert len(version) > 0
    # Verify version follows semver format (X.Y.Z) without hard-coding specific version
    parts = version.split(".")
    assert len(parts) >= 2, f"Version should have at least major.minor: {version}"
    assert parts[0].isdigit(), f"Major version should be numeric: {version}"
    assert parts[1].isdigit(), f"Minor version should be numeric: {version}"


def test_version_attribute():
    """Test __version__ attribute."""
    assert hasattr(cqlite, "__version__")
    assert isinstance(cqlite.__version__, str)
    assert cqlite.__version__ == cqlite.version()
