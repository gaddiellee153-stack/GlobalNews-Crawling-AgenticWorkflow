"""P1 Tests — L0d Content Structure Validation (validate_output_structure in _context_lib.py)

Tests for:
  - Config absence → (True, []) — backward compatible
  - Config empty/invalid YAML → (True, [])
  - Step not defined → (True, [])
  - Heading check PASS/FAIL (case-insensitive)
  - Marker check PASS/FAIL (case-insensitive substring)
  - Count check PASS/FAIL (exact boundary: min_count, min_count-1, min_count+1)
  - Combined checks (partial pass → only failed checks in warnings)
  - Invalid regex pattern → that check skipped, rest continue
"""

import importlib.util
import os
import sys

import pytest
import yaml

# Import _context_lib module
HOOKS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    ".claude", "hooks", "scripts",
)


def _import_hook(name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(HOOKS_DIR, f"{name}.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ctx = _import_hook("_context_lib")


# =========================================================================
# Fixtures
# =========================================================================

@pytest.fixture
def tmp_proj(tmp_path):
    """Create a temporary project with config/ and SOT."""
    (tmp_path / "config").mkdir()
    (tmp_path / ".claude").mkdir()
    return tmp_path


def _write_config(tmp_proj, steps_config):
    """Write an output-structure.yaml with given steps config."""
    config_path = tmp_proj / "config" / "output-structure.yaml"
    config_path.write_text(yaml.dump({"steps": steps_config}))
    return config_path


def _write_output(tmp_proj, content, filename="output.md"):
    """Write an output file and return its path."""
    out_path = tmp_proj / filename
    out_path.write_text(content)
    return str(out_path)


# =========================================================================
# Backward Compatibility
# =========================================================================

class TestBackwardCompat:
    """Config absence or invalid config → graceful skip."""

    def test_no_config_file(self, tmp_proj):
        is_valid, warnings = ctx.validate_output_structure(
            str(tmp_proj), 1, output_path="/tmp/any.md"
        )
        assert is_valid is True
        assert warnings == []

    def test_empty_yaml(self, tmp_proj):
        (tmp_proj / "config" / "output-structure.yaml").write_text("")
        is_valid, warnings = ctx.validate_output_structure(
            str(tmp_proj), 1, output_path="/tmp/any.md"
        )
        assert is_valid is True
        assert warnings == []

    def test_invalid_yaml(self, tmp_proj):
        (tmp_proj / "config" / "output-structure.yaml").write_text(": : invalid: [")
        is_valid, warnings = ctx.validate_output_structure(
            str(tmp_proj), 1, output_path="/tmp/any.md"
        )
        assert is_valid is True
        assert warnings == []

    def test_missing_steps_key(self, tmp_proj):
        (tmp_proj / "config" / "output-structure.yaml").write_text(
            yaml.dump({"other": "data"})
        )
        is_valid, warnings = ctx.validate_output_structure(
            str(tmp_proj), 1, output_path="/tmp/any.md"
        )
        assert is_valid is True
        assert warnings == []

    def test_step_not_defined(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [{"type": "marker", "pattern": "x"}]}})
        out = _write_output(tmp_proj, "some content")
        is_valid, warnings = ctx.validate_output_structure(
            str(tmp_proj), 99, output_path=out
        )
        assert is_valid is True
        assert warnings == []

    def test_output_file_not_found(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [{"type": "marker", "pattern": "x"}]}})
        is_valid, warnings = ctx.validate_output_structure(
            str(tmp_proj), 1, output_path="/nonexistent/file.md"
        )
        assert is_valid is True
        assert warnings == []


# =========================================================================
# Heading Checks
# =========================================================================

class TestHeadingCheck:
    """L0d-heading: regex search for markdown headings."""

    def test_heading_pass(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "heading", "pattern": "## .*(RSS|Sitemap)", "description": "RSS section"}
        ]}})
        out = _write_output(tmp_proj, "# Title\n\n## RSS Feed Analysis\n\nContent here.")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True
        assert warnings == []

    def test_heading_fail(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "heading", "pattern": "## .*(RSS|Sitemap)", "description": "RSS section"}
        ]}})
        out = _write_output(tmp_proj, "# Title\n\n## Bot Blocking\n\nNo RSS here.")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is False
        assert len(warnings) == 1
        assert "L0d-heading WARNING" in warnings[0]

    def test_heading_case_insensitive(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "heading", "pattern": "## .*(rss|sitemap)", "description": "RSS"}
        ]}})
        out = _write_output(tmp_proj, "## RSS FEED ANALYSIS\n\nContent.")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True


# =========================================================================
# Marker Checks
# =========================================================================

class TestMarkerCheck:
    """L0d-marker: literal substring presence check."""

    def test_marker_pass(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "marker", "pattern": "mermaid", "description": "diagram"}
        ]}})
        out = _write_output(tmp_proj, "```mermaid\ngraph TD\nA-->B\n```")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True

    def test_marker_fail(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "marker", "pattern": "mermaid", "description": "diagram"}
        ]}})
        out = _write_output(tmp_proj, "No diagrams here, just text.")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is False
        assert "L0d-marker WARNING" in warnings[0]

    def test_marker_case_insensitive(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "marker", "pattern": "MERMAID", "description": "diagram"}
        ]}})
        out = _write_output(tmp_proj, "```mermaid\n```")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True


# =========================================================================
# Count Checks
# =========================================================================

class TestCountCheck:
    """L0d-count: regex findall count vs min_count."""

    def test_count_pass_exact(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "count", "pattern": "^\\|[^-].*\\|", "min_count": 3, "description": "rows"}
        ]}})
        content = "| Header | Col |\n|--------|-----|\n| Data 1 | A |\n| Data 2 | B |\n| Data 3 | C |\n"
        out = _write_output(tmp_proj, content)
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        # Header + 3 data rows = 4 matches, min_count=3 → PASS
        assert is_valid is True

    def test_count_fail_below(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "count", "pattern": "^\\|[^-].*\\|", "min_count": 10, "description": "rows"}
        ]}})
        content = "| Header | Col |\n|--------|-----|\n| Data 1 | A |\n"
        out = _write_output(tmp_proj, content)
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is False
        assert "L0d-count WARNING" in warnings[0]

    def test_count_boundary_exactly_min(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "count", "pattern": "PASS", "min_count": 3, "description": "pass markers"}
        ]}})
        out = _write_output(tmp_proj, "PASS\nPASS\nPASS\n")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True
        assert warnings == []

    def test_count_boundary_one_below(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "count", "pattern": "PASS", "min_count": 3, "description": "pass markers"}
        ]}})
        out = _write_output(tmp_proj, "PASS\nPASS\n")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is False


# =========================================================================
# Combined / Edge Cases
# =========================================================================

class TestCombinedAndEdge:
    """Combined checks and edge cases."""

    def test_partial_pass(self, tmp_proj):
        """Multiple checks — only failed ones appear in warnings."""
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "heading", "pattern": "## RSS", "description": "RSS"},
            {"type": "marker", "pattern": "mermaid", "description": "diagram"},
        ]}})
        out = _write_output(tmp_proj, "## RSS Feed\n\nNo diagram here.")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is False
        assert len(warnings) == 1
        assert "mermaid" in warnings[0]

    def test_invalid_regex_skipped(self, tmp_proj):
        """Invalid regex pattern → that check skipped, rest continue."""
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "heading", "pattern": "[invalid(regex", "description": "bad"},
            {"type": "marker", "pattern": "hello", "description": "good"},
        ]}})
        out = _write_output(tmp_proj, "hello world")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        # Invalid regex skipped, marker passes
        assert is_valid is True
        assert warnings == []

    def test_empty_pattern_skipped(self, tmp_proj):
        _write_config(tmp_proj, {1: {"checks": [
            {"type": "heading", "pattern": "", "description": "empty"},
            {"type": "marker", "pattern": "test", "description": "ok"},
        ]}})
        out = _write_output(tmp_proj, "test content")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True

    def test_step_as_string_key(self, tmp_proj):
        """Config may use string step keys (YAML coercion)."""
        _write_config(tmp_proj, {"1": {"checks": [
            {"type": "marker", "pattern": "hello", "description": "test"}
        ]}})
        out = _write_output(tmp_proj, "hello world")
        is_valid, warnings = ctx.validate_output_structure(str(tmp_proj), 1, output_path=out)
        assert is_valid is True
