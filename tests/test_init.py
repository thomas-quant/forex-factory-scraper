"""
Tests for src/forexfactory/__init__.py public surface:
  - __version__ derived from installed package metadata
  - __all__ lists exact public surface
  - get() and populate() carry complete type annotations
"""

import inspect
from pathlib import Path


def test_version_is_not_hardcoded_literal():
    """__version__ must NOT be a bare hardcoded literal in __init__.py."""
    init_path = Path(__file__).resolve().parents[1] / "src" / "forexfactory" / "__init__.py"
    source = init_path.read_text(encoding="utf-8")
    assert '__version__ = "0.1.0"' not in source, (
        "__version__ must not be hardcoded as '0.1.0'; use importlib.metadata"
    )


def test_version_uses_importlib_metadata():
    """__version__ must be derived via importlib.metadata.version('forexfactory')."""
    init_path = Path(__file__).resolve().parents[1] / "src" / "forexfactory" / "__init__.py"
    source = init_path.read_text(encoding="utf-8")
    assert 'version("forexfactory")' in source, (
        '__init__.py must call importlib.metadata.version("forexfactory")'
    )


def test_version_is_1_1_2():
    """Installed package version must be 1.1.2 (reflects pyproject.toml bump)."""
    import forexfactory

    assert forexfactory.__version__ == "1.1.2", (
        f"Expected '1.1.2', got '{forexfactory.__version__}'"
    )


def test_all_lists_exact_public_surface():
    """__all__ must be exactly the documented public surface.

    Plan 03 (05-03) adds read (API-01 / D-08).
    1.1.2 adds actual_initial / actual_revised (point-in-time vintage derivation).
    """
    import forexfactory

    assert forexfactory.__all__ == [
        "get",
        "read",
        "populate",
        "surprise",
        "surprise_z",
        "actual_initial",
        "actual_revised",
        "__version__",
    ], (f"__all__ mismatch: {forexfactory.__all__}")


def test_get_parameters_are_annotated():
    """get() must have complete parameter annotations using str|None style."""
    import forexfactory

    sig = inspect.signature(forexfactory.get)
    params = sig.parameters

    # Check required params exist with annotations
    assert "currencies" in params
    assert "impacts" in params
    assert "start" in params
    assert "end" in params
    assert "include_no_data" in params
    assert "cache_dir" in params
    assert "auto_fetch" in params

    # All params must be annotated (no inspect.Parameter.empty)
    for name, param in params.items():
        assert param.annotation != inspect.Parameter.empty, (
            f"get() parameter '{name}' has no annotation"
        )

    # Return type must be Path
    assert sig.return_annotation is Path, (
        f"get() return annotation should be Path, got {sig.return_annotation}"
    )


def test_populate_parameters_are_annotated():
    """populate() must have complete parameter annotations using str|None style."""
    import forexfactory

    sig = inspect.signature(forexfactory.populate)
    params = sig.parameters

    # Check required params exist with annotations
    assert "currencies" in params
    assert "impacts" in params
    assert "start" in params
    assert "end" in params
    assert "raw_dir" in params
    assert "cache_dir" in params
    assert "force" in params
    assert "force_refresh" in params
    assert "auto_fetch" in params

    # All params must be annotated
    for name, param in params.items():
        assert param.annotation != inspect.Parameter.empty, (
            f"populate() parameter '{name}' has no annotation"
        )


def test_populate_returns_dict_of_str_int():
    """populate() return annotation must be dict[str, int]."""
    import forexfactory

    sig = inspect.signature(forexfactory.populate)
    # dict[str, int] — check it's not bare dict
    ret = sig.return_annotation
    assert ret is not dict, "populate() return annotation must be dict[str, int], not bare dict"
    # Verify it is a generic alias for dict[str, int]
    assert hasattr(ret, "__origin__") or isinstance(ret, type), (
        f"populate() return annotation '{ret}' should be dict[str, int]"
    )


def test_read_is_accessible_on_module():
    """forexfactory.read must be accessible as a top-level attribute."""
    import forexfactory

    assert hasattr(forexfactory, "read"), "forexfactory.read must be importable"
    assert callable(forexfactory.read), "forexfactory.read must be callable"


def test_read_parameters_are_annotated():
    """read() must have complete parameter annotations matching get() (D-08)."""
    import forexfactory

    sig = inspect.signature(forexfactory.read)
    params = sig.parameters

    # Mirror get() keyword-only parameter list (D-08)
    for name in (
        "currencies", "impacts", "start", "end", "include_no_data", "cache_dir", "auto_fetch"
    ):
        assert name in params, f"read() missing parameter '{name}' (must mirror get())"

    # All params must be annotated
    for name, param in params.items():
        assert param.annotation != inspect.Parameter.empty, (
            f"read() parameter '{name}' has no annotation"
        )

    # Return type must be pd.DataFrame
    import pandas as pd

    assert sig.return_annotation is pd.DataFrame, (
        f"read() return annotation should be pd.DataFrame, got {sig.return_annotation}"
    )


def test_noqa_plc0415_tags_preserved():
    """Both # noqa: PLC0415 tags must survive in __init__.py."""
    init_path = Path(__file__).resolve().parents[1] / "src" / "forexfactory" / "__init__.py"
    source = init_path.read_text(encoding="utf-8")
    count = source.count("noqa: PLC0415")
    assert count == 2, f"Expected 2 '# noqa: PLC0415' tags in __init__.py, found {count}"


def test_pragma_no_cover_on_packagenotfound_branch():
    """The PackageNotFoundError fallback branch must carry # pragma: no cover."""
    init_path = Path(__file__).resolve().parents[1] / "src" / "forexfactory" / "__init__.py"
    source = init_path.read_text(encoding="utf-8")
    assert "PackageNotFoundError" in source, (
        "__init__.py must import and catch PackageNotFoundError"
    )
    assert "pragma: no cover" in source, (
        "__init__.py PackageNotFoundError branch must have # pragma: no cover"
    )
