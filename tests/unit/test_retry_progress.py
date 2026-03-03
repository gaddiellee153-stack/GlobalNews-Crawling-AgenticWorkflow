"""P1 Tests — Circuit Breaker (retry progress detection in validate_retry_budget.py)

Tests for:
  - RP1: JSONL history parsing (empty, malformed, normal)
  - RP2: Progress measurement (ascending, descending, stagnant, oscillating, boundary)
  - RP3: Circuit breaker state (OPEN/CLOSED boundary: exactly 3 attempts, 2 attempts, delta exactly 5)
  - CLI flag integration (--check-progress, --record-attempt --pacs-score)
  - Argument validation (--record-attempt without --pacs-score → error)
  - Existing functionality regression (--check-and-increment, --increment, default mode)
"""

import importlib.util
import json
import os
import subprocess
import sys

import pytest

# Import validate_retry_budget module
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


vrb = _import_hook("validate_retry_budget")
gcs = _import_hook("generate_context_summary")


# =========================================================================
# Fixtures
# =========================================================================

@pytest.fixture
def tmp_proj(tmp_path):
    """Create a temporary project with gate log directories."""
    for gate_dir in ("verification-logs", "pacs-logs", "review-logs"):
        (tmp_path / gate_dir).mkdir()
    # Create snapshot for ULW detection (ULW inactive by default)
    snapshots_dir = tmp_path / ".claude" / "context-snapshots"
    snapshots_dir.mkdir(parents=True)
    (snapshots_dir / "latest.md").write_text("# Snapshot\nNo ULW here.\n")
    return tmp_path


@pytest.fixture
def history_file(tmp_proj):
    """Return a factory that creates a JSONL history file."""
    def _create(step, gate, entries):
        path = vrb._history_path(str(tmp_proj), step, gate)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")
        return path
    return _create


# =========================================================================
# RP1: History File Parsing
# =========================================================================

class TestRP1Parsing:
    """RP1: JSONL history parsing robustness."""

    def test_empty_file(self, tmp_proj, history_file):
        history_file(1, "pacs", [])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["circuit_breaker"] == "CLOSED"
        assert result["history_length"] == 0

    def test_no_history_file(self, tmp_proj):
        result = vrb._validate_retry_progress(str(tmp_proj), 99, "pacs")
        assert result["circuit_breaker"] == "CLOSED"
        assert result["history_length"] == 0

    def test_malformed_lines_skipped(self, tmp_proj):
        path = vrb._history_path(str(tmp_proj), 1, "pacs")
        with open(path, "w") as f:
            f.write("not json\n")
            f.write('{"pacs_score": 45}\n')
            f.write("also bad\n")
            f.write('{"pacs_score": 50}\n')
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["history_length"] == 2
        assert result["recent_scores"] == [45, 50]

    def test_normal_jsonl(self, tmp_proj, history_file):
        entries = [
            {"pacs_score": 40, "timestamp": "2026-01-01T00:00:00Z"},
            {"pacs_score": 45, "timestamp": "2026-01-01T01:00:00Z"},
            {"pacs_score": 50, "timestamp": "2026-01-01T02:00:00Z"},
        ]
        history_file(1, "pacs", entries)
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["history_length"] == 3
        assert result["recent_scores"] == [40, 45, 50]

    def test_single_entry(self, tmp_proj, history_file):
        history_file(1, "pacs", [{"pacs_score": 40}])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["circuit_breaker"] == "CLOSED"
        assert result["history_length"] == 1

    def test_missing_pacs_score_key(self, tmp_proj):
        path = vrb._history_path(str(tmp_proj), 1, "pacs")
        with open(path, "w") as f:
            f.write('{"gate_result": "FAIL"}\n')  # no pacs_score
            f.write('{"pacs_score": 45}\n')
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["history_length"] == 1  # Only the entry with pacs_score

    def test_float_score_accepted(self, tmp_proj, history_file):
        history_file(1, "pacs", [{"pacs_score": 45.5}, {"pacs_score": 48.7}])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["history_length"] == 2
        assert result["recent_scores"] == [45, 48]  # Converted to int


# =========================================================================
# RP2: Progress Measurement
# =========================================================================

class TestRP2ProgressMeasurement:
    """RP2: Consecutive pACS delta calculation."""

    def test_ascending_scores(self, tmp_proj, history_file):
        history_file(1, "pacs", [
            {"pacs_score": 30}, {"pacs_score": 40},
            {"pacs_score": 55}, {"pacs_score": 70},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["recent_deltas"] == [10, 15, 15]
        assert result["circuit_breaker"] == "CLOSED"

    def test_descending_scores(self, tmp_proj, history_file):
        """Descending = negative deltas, all <= threshold → OPEN."""
        history_file(1, "pacs", [
            {"pacs_score": 50}, {"pacs_score": 48},
            {"pacs_score": 45}, {"pacs_score": 44},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: -2, -3, -1 — all <= 5
        assert result["circuit_breaker"] == "OPEN"

    def test_stagnant_scores(self, tmp_proj, history_file):
        """Same score 4 times → all deltas 0 → OPEN."""
        history_file(1, "pacs", [
            {"pacs_score": 45}, {"pacs_score": 45},
            {"pacs_score": 45}, {"pacs_score": 45},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["circuit_breaker"] == "OPEN"
        assert result["recent_deltas"] == [0, 0, 0]

    def test_oscillating_no_progress(self, tmp_proj, history_file):
        """Oscillating within threshold → OPEN."""
        history_file(1, "pacs", [
            {"pacs_score": 45}, {"pacs_score": 47},
            {"pacs_score": 44}, {"pacs_score": 46},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: 2, -3, 2 — all <= 5
        assert result["circuit_breaker"] == "OPEN"


# =========================================================================
# RP3: Circuit Breaker State
# =========================================================================

class TestRP3CircuitBreaker:
    """RP3: Circuit breaker OPEN/CLOSED boundary conditions."""

    def test_exactly_3_no_progress_deltas(self, tmp_proj, history_file):
        """Exactly 3 consecutive no-progress → OPEN."""
        history_file(1, "pacs", [
            {"pacs_score": 40}, {"pacs_score": 43},
            {"pacs_score": 44}, {"pacs_score": 45},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: 3, 1, 1 — all <= 5
        assert result["circuit_breaker"] == "OPEN"

    def test_exactly_2_no_progress_deltas(self, tmp_proj, history_file):
        """Only 2 entries → 1 delta → not enough for window of 3 → CLOSED."""
        history_file(1, "pacs", [
            {"pacs_score": 40}, {"pacs_score": 43}, {"pacs_score": 44},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: 3, 1 — only 2, window is 3
        assert result["circuit_breaker"] == "CLOSED"

    def test_delta_exactly_5(self, tmp_proj, history_file):
        """Delta exactly at threshold (5) → counts as no-progress."""
        history_file(1, "pacs", [
            {"pacs_score": 40}, {"pacs_score": 45},
            {"pacs_score": 50}, {"pacs_score": 55},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: 5, 5, 5 — all == 5, threshold is <= 5
        assert result["circuit_breaker"] == "OPEN"

    def test_delta_exactly_6(self, tmp_proj, history_file):
        """Delta just above threshold (6) → real progress → CLOSED."""
        history_file(1, "pacs", [
            {"pacs_score": 40}, {"pacs_score": 46},
            {"pacs_score": 52}, {"pacs_score": 58},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: 6, 6, 6 — all > 5
        assert result["circuit_breaker"] == "CLOSED"

    def test_one_progress_break_in_window(self, tmp_proj, history_file):
        """One delta > threshold breaks the no-progress chain → CLOSED."""
        history_file(1, "pacs", [
            {"pacs_score": 40}, {"pacs_score": 43},
            {"pacs_score": 55},  # +12 = progress!
            {"pacs_score": 57},
        ])
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        # Deltas: 3, 12, 2 — 12 > 5, so recent 3 not all no-progress
        assert result["circuit_breaker"] == "CLOSED"

    def test_different_gates_independent(self, tmp_proj, history_file):
        """Each gate has independent history."""
        history_file(1, "pacs", [
            {"pacs_score": 45}, {"pacs_score": 45},
            {"pacs_score": 45}, {"pacs_score": 45},
        ])
        # Verification gate has no history
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "verification")
        assert result["circuit_breaker"] == "CLOSED"

        # pacs gate is stalled
        result = vrb._validate_retry_progress(str(tmp_proj), 1, "pacs")
        assert result["circuit_breaker"] == "OPEN"


# =========================================================================
# Record Attempt
# =========================================================================

class TestRecordAttempt:
    """Test _record_retry_attempt function."""

    def test_creates_file_and_appends(self, tmp_proj):
        vrb._record_retry_attempt(str(tmp_proj), 1, "pacs", 45, "F", "FAIL")
        vrb._record_retry_attempt(str(tmp_proj), 1, "pacs", 50, "C", "FAIL")
        path = vrb._history_path(str(tmp_proj), 1, "pacs")
        assert os.path.exists(path)
        with open(path) as f:
            lines = [l.strip() for l in f if l.strip()]
        assert len(lines) == 2
        entry1 = json.loads(lines[0])
        assert entry1["pacs_score"] == 45
        assert entry1["weak_dimension"] == "F"
        entry2 = json.loads(lines[1])
        assert entry2["pacs_score"] == 50

    def test_no_weak_dimension(self, tmp_proj):
        vrb._record_retry_attempt(str(tmp_proj), 1, "pacs", 45)
        path = vrb._history_path(str(tmp_proj), 1, "pacs")
        with open(path) as f:
            entry = json.loads(f.readline())
        assert "weak_dimension" not in entry
        assert entry["gate_result"] == "FAIL"  # default


# =========================================================================
# CLI Flag Integration (subprocess)
# =========================================================================

class TestCLIFlags:
    """CLI integration tests via subprocess."""

    def _run(self, args, cwd=None):
        cmd = [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py")] + args
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
        return result

    def test_check_progress_empty(self, tmp_proj):
        result = self._run([
            "--step", "1", "--gate", "pacs",
            "--project-dir", str(tmp_proj),
            "--check-progress",
        ])
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["valid"] is True
        assert data["circuit_breaker"] == "CLOSED"

    def test_record_attempt(self, tmp_proj):
        result = self._run([
            "--step", "1", "--gate", "pacs",
            "--project-dir", str(tmp_proj),
            "--record-attempt", "--pacs-score", "45",
            "--weak-dimension", "F",
        ])
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["valid"] is True
        assert data["recorded"] is True
        assert data["pacs_score"] == 45

    def test_record_attempt_without_pacs_score_fails(self, tmp_proj):
        result = self._run([
            "--step", "1", "--gate", "pacs",
            "--project-dir", str(tmp_proj),
            "--record-attempt",
        ])
        assert result.returncode != 0  # argparse error

    def test_check_progress_after_records(self, tmp_proj):
        # Record 4 stagnant attempts
        for score in [45, 45, 45, 45]:
            self._run([
                "--step", "1", "--gate", "pacs",
                "--project-dir", str(tmp_proj),
                "--record-attempt", "--pacs-score", str(score),
            ])
        result = self._run([
            "--step", "1", "--gate", "pacs",
            "--project-dir", str(tmp_proj),
            "--check-progress",
        ])
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["circuit_breaker"] == "OPEN"


# =========================================================================
# P1-1: --check-and-increment with integrated Circuit Breaker
# =========================================================================

class TestCheckAndIncrementWithCB:
    """P1-1: --check-and-increment enforces BOTH budget AND circuit breaker."""

    def test_budget_ok_cb_closed_no_history(self, tmp_proj):
        """No history file → CB CLOSED → normal increment."""
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["can_retry"] is True
        assert data["incremented"] is True
        assert data["circuit_breaker"] == "CLOSED"

    def test_budget_ok_cb_open_blocks_retry(self, tmp_proj, history_file):
        """Budget allows but CB OPEN → can_retry: false, counter NOT incremented."""
        # Create stagnant history → CB OPEN
        history_file(1, "pacs", [
            {"pacs_score": 45}, {"pacs_score": 45},
            {"pacs_score": 45}, {"pacs_score": 45},
        ])
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["can_retry"] is False
        assert data["reason"] == "circuit_breaker_open"
        assert data["incremented"] is False
        assert data["circuit_breaker"] == "OPEN"

    def test_cb_open_preserves_budget(self, tmp_proj, history_file):
        """CB blocks → counter stays at 0 (budget preserved for after user intervention)."""
        history_file(1, "pacs", [
            {"pacs_score": 45}, {"pacs_score": 46},
            {"pacs_score": 44}, {"pacs_score": 45},
        ])
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        data = json.loads(result.stdout)
        assert data["retries_used"] == 0  # NOT incremented
        assert data["budget_remaining"] == 10  # Full budget preserved

    def test_budget_exhausted_skips_cb_check(self, tmp_proj):
        """Budget exhausted → reason: budget_exhausted (CB not checked)."""
        # Exhaust budget
        counter_path = vrb._counter_path(str(tmp_proj), 1, "pacs")
        os.makedirs(os.path.dirname(counter_path), exist_ok=True)
        with open(counter_path, "w") as f:
            f.write("10")  # DEFAULT_MAX_RETRIES = 10
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        data = json.loads(result.stdout)
        assert data["can_retry"] is False
        assert data["reason"] == "budget_exhausted"

    def test_cb_closed_with_progress(self, tmp_proj, history_file):
        """CB CLOSED (good progress) → normal increment."""
        history_file(1, "pacs", [
            {"pacs_score": 30}, {"pacs_score": 40},
            {"pacs_score": 55}, {"pacs_score": 70},
        ])
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        data = json.loads(result.stdout)
        assert data["can_retry"] is True
        assert data["incremented"] is True
        assert data["circuit_breaker"] == "CLOSED"

    def test_reason_field_absent_when_can_retry(self, tmp_proj):
        """can_retry: true → no reason field in output."""
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        data = json.loads(result.stdout)
        assert data["can_retry"] is True
        assert "reason" not in data

    def test_default_mode_includes_cb_state(self, tmp_proj, history_file):
        """Default read-only mode also reports circuit breaker state."""
        history_file(1, "pacs", [
            {"pacs_score": 45}, {"pacs_score": 45},
            {"pacs_score": 45}, {"pacs_score": 45},
        ])
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj)],
            capture_output=True, text=True,
        )
        data = json.loads(result.stdout)
        # Default mode reports CB state but does NOT block (only --check-and-increment blocks)
        assert data["circuit_breaker"] == "OPEN"
        assert data["can_retry"] is True  # Read-only doesn't enforce CB

    def test_recent_scores_in_output(self, tmp_proj, history_file):
        """CB output includes recent_scores and recent_deltas for transparency."""
        history_file(1, "pacs", [
            {"pacs_score": 40}, {"pacs_score": 43},
            {"pacs_score": 44}, {"pacs_score": 45},
        ])
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        data = json.loads(result.stdout)
        assert "recent_scores" in data
        assert "recent_deltas" in data


# =========================================================================
# Regression: Existing Functionality
# =========================================================================

class TestRegression:
    """Existing --check-and-increment and --increment must still work."""

    def test_default_read_only(self, tmp_proj):
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "verification",
             "--project-dir", str(tmp_proj)],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["can_retry"] is True
        assert data["retries_used"] == 0
        assert data["incremented"] is False
        # Default mode now includes circuit breaker state
        assert data["circuit_breaker"] == "CLOSED"

    def test_check_and_increment(self, tmp_proj):
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "verification",
             "--project-dir", str(tmp_proj),
             "--check-and-increment"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["can_retry"] is True
        assert data["retries_used"] == 1
        assert data["incremented"] is True

    def test_mutually_exclusive_flags(self, tmp_proj):
        """Cannot combine --check-and-increment with --check-progress."""
        result = subprocess.run(
            [sys.executable, os.path.join(HOOKS_DIR, "validate_retry_budget.py"),
             "--step", "1", "--gate", "pacs",
             "--project-dir", str(tmp_proj),
             "--check-and-increment", "--check-progress"],
            capture_output=True, text=True,
        )
        assert result.returncode != 0


# =========================================================================
# Safety Net: _check_missing_retry_records (generate_context_summary.py)
# =========================================================================

class TestCheckMissingRetryRecords:
    """Tests for the Stop-hook safety net that detects missing --record-attempt calls."""

    def test_empty_gate_dirs(self, tmp_proj, capsys):
        """No counter files → no warnings."""
        gcs._check_missing_retry_records(str(tmp_proj))
        captured = capsys.readouterr()
        assert "RETRY RECORD GAP" not in captured.err

    def test_counter_only_no_history(self, tmp_proj, capsys):
        """Counter=3 but no history file → gap of 3 > 1+1 → warning."""
        counter_path = tmp_proj / "pacs-logs" / ".step-5-retry-count"
        counter_path.write_text("3")
        gcs._check_missing_retry_records(str(tmp_proj))
        captured = capsys.readouterr()
        assert "RETRY RECORD GAP" in captured.err
        assert "step-5/pacs" in captured.err
        assert "counter=3" in captured.err

    def test_counter_matches_history(self, tmp_proj, capsys):
        """Counter=3 and history has 3 entries → no gap → no warning."""
        counter_path = tmp_proj / "pacs-logs" / ".step-2-retry-count"
        counter_path.write_text("3")
        history_path = tmp_proj / "pacs-logs" / ".step-2-retry-history.jsonl"
        lines = [json.dumps({"pacs_score": s}) + "\n" for s in [40, 42, 44]]
        history_path.write_text("".join(lines))
        gcs._check_missing_retry_records(str(tmp_proj))
        captured = capsys.readouterr()
        assert "RETRY RECORD GAP" not in captured.err

    def test_delta_of_one_tolerated(self, tmp_proj, capsys):
        """Counter=3 and history has 2 entries → delta=1 → tolerated (mid-rework)."""
        counter_path = tmp_proj / "review-logs" / ".step-1-retry-count"
        counter_path.write_text("3")
        history_path = tmp_proj / "review-logs" / ".step-1-retry-history.jsonl"
        lines = [json.dumps({"pacs_score": s}) + "\n" for s in [40, 42]]
        history_path.write_text("".join(lines))
        gcs._check_missing_retry_records(str(tmp_proj))
        captured = capsys.readouterr()
        assert "RETRY RECORD GAP" not in captured.err

    def test_non_numeric_step_ignored(self, tmp_proj, capsys):
        """Files with non-numeric step part are silently skipped."""
        bad_file = tmp_proj / "pacs-logs" / ".step-abc-retry-count"
        bad_file.write_text("5")
        gcs._check_missing_retry_records(str(tmp_proj))
        captured = capsys.readouterr()
        assert "RETRY RECORD GAP" not in captured.err

    def test_counter_zero_skipped(self, tmp_proj, capsys):
        """Counter=0 means no retries yet → skip entirely."""
        counter_path = tmp_proj / "verification-logs" / ".step-1-retry-count"
        counter_path.write_text("0")
        gcs._check_missing_retry_records(str(tmp_proj))
        captured = capsys.readouterr()
        assert "RETRY RECORD GAP" not in captured.err
