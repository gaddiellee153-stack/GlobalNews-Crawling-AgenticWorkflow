#!/usr/bin/env python3
"""
Retry Budget P1 Validation — validate_retry_budget.py

Standalone script called by Orchestrator before each retry attempt.
NOT a Hook — manually invoked during workflow execution.

Usage:
    # Check if retry is allowed (read-only)
    python3 .claude/hooks/scripts/validate_retry_budget.py --step 3 --gate verification --project-dir .

    # Check AND atomically consume one retry budget (RECOMMENDED — single call)
    python3 .claude/hooks/scripts/validate_retry_budget.py --step 3 --gate verification --project-dir . --check-and-increment

    # Increment only (legacy — prefer --check-and-increment)
    python3 .claude/hooks/scripts/validate_retry_budget.py --step 3 --gate verification --project-dir . --increment

    # Check circuit breaker (read-only — no counter modification)
    python3 .claude/hooks/scripts/validate_retry_budget.py --step 3 --gate pacs --project-dir . --check-progress

    # Record retry attempt with pACS score (after rework completes)
    python3 .claude/hooks/scripts/validate_retry_budget.py --step 3 --gate pacs --project-dir . --record-attempt --pacs-score 45 --weak-dimension F

Output: JSON to stdout
    {"valid": true, "can_retry": true, "retries_used": 1, "max_retries": 3, ...}

Exit codes:
    0 — validation completed (check "can_retry" field for decision)
    1 — argument error or fatal failure

Checks (RB1-RB3):
    RB1: Counter file read (deterministic integer, 0 if absent)
    RB2: ULW active detection (snapshot regex — reuses existing P1 pattern)
    RB3: Budget comparison (retries_used < max_retries)

Circuit Breaker (RP1-RP3):
    RP1: History file parse (JSONL — malformed lines skipped)
    RP2: Consecutive pACS delta calculation (pure arithmetic)
    RP3: NO_PROGRESS_WINDOW consecutive delta <= NO_PROGRESS_THRESHOLD → OPEN

Modes:
    (default)              Read-only — check budget + circuit breaker state (no counter change)
    --check-and-increment  Atomic check+consume — checks BOTH budget AND circuit breaker.
                           If budget exhausted OR circuit breaker OPEN → can_retry: false
                           (counter NOT incremented — budget preserved).
                           If both allow → increment counter and return can_retry: true.
                           P1: Single call enforces all retry conditions — LLM cannot bypass.
    --increment            Unconditional increment — legacy mode for manual use
    --check-progress       Read-only — check circuit breaker state only (diagnostic/debug)
    --record-attempt       Record a retry attempt with pACS score to history file
                           (requires --pacs-score; optional --weak-dimension, --gate-result)

P1 Compliance: All logic is deterministic arithmetic + file I/O + regex.
SOT Compliance: Read-only on SOT. Writes only to {gate}-logs/ counter/history files.

Known limitation: ULW detection reads latest.md snapshot. If a previous session
used ULW and the snapshot hasn't been overwritten yet, max_retries may be 15
instead of 10. This is a safe-direction false positive (allows 5 extra retries).
"""

import argparse
import json
import os
import re
import sys

# Constants
DEFAULT_MAX_RETRIES = 10
ULW_MAX_RETRIES = 15
VALID_GATES = ("verification", "pacs", "review")

# --- Retry Progress Circuit Breaker Constants (P1) ---
# Self-contained in this script — no D-7 duplication needed
# (functions also self-contained below)
NO_PROGRESS_THRESHOLD = 5    # pACS score improvement threshold (points)
NO_PROGRESS_WINDOW = 3       # Consecutive no-progress attempts → circuit breaker OPEN

# Gate → directory mapping
# D-7: duplicated in generate_context_summary.py _check_missing_retry_records()
GATE_DIRS = {
    "verification": "verification-logs",
    "pacs": "pacs-logs",
    "review": "review-logs",
}

# ULW detection regex — matches "ULW 상태" section in snapshot
# Reuses the same signal that restore_context.py checks
_ULW_SNAPSHOT_RE = re.compile(r"ULW 상태|Ultrawork Mode State")


def _counter_path(project_dir, step, gate):
    """Return the path to the retry counter file for a step/gate."""
    gate_dir = os.path.join(project_dir, GATE_DIRS[gate])
    return os.path.join(gate_dir, f".step-{step}-retry-count")


def _read_counter(path):
    """Read retry count from counter file. Returns 0 if absent or invalid.

    P1: deterministic file read + int parse.
    """
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return int(f.read().strip())
    except (ValueError, IOError):
        pass
    return 0


def _increment_counter(path):
    """Atomically increment the retry counter and return the new value.

    P1: atomic write (temp → rename) to prevent partial writes.
    """
    current = _read_counter(path)
    new_value = current + 1

    # Ensure directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Atomic write
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            f.write(str(new_value))
        os.replace(tmp_path, path)
    except Exception:
        # Fallback: direct write
        try:
            with open(path, "w") as f:
                f.write(str(new_value))
        except IOError:
            pass

    return new_value


def _detect_ulw_from_snapshot(project_dir):
    """Detect ULW active state from latest snapshot.

    P1: regex match on file content — same signal as restore_context.py.
    Returns True if ULW is active, False otherwise.
    """
    snapshot_path = os.path.join(
        project_dir, ".claude", "context-snapshots", "latest.md"
    )
    try:
        if os.path.exists(snapshot_path):
            with open(snapshot_path, "r", encoding="utf-8") as f:
                content = f.read()
            return bool(_ULW_SNAPSHOT_RE.search(content))
    except IOError:
        pass
    return False


def _history_path(project_dir, step, gate):
    """Return the path to the retry history JSONL file for a step/gate.

    Reuses GATE_DIRS mapping from _counter_path().
    """
    gate_dir = os.path.join(project_dir, GATE_DIRS[gate])
    return os.path.join(gate_dir, f".step-{step}-retry-history.jsonl")


def _record_retry_attempt(project_dir, step, gate, pacs_score,
                          weak_dimension=None, gate_result="FAIL"):
    """Record a retry attempt with pACS score to history JSONL file.

    P1: Deterministic file append — no LLM interpretation.
    SOT Compliance: Writes only to {gate}-logs/ (NOT SOT).

    Args:
        project_dir: Project root directory
        step: Step number
        gate: Quality gate type
        pacs_score: pACS score (int, 0-100)
        weak_dimension: Weakest dimension (F/C/L, optional)
        gate_result: Gate result string (default: "FAIL")
    """
    import datetime

    path = _history_path(project_dir, step, gate)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    entry = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "pacs_score": pacs_score,
        "gate_result": gate_result,
    }
    if weak_dimension:
        entry["weak_dimension"] = weak_dimension

    line = json.dumps(entry, ensure_ascii=False) + "\n"
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except IOError:
        pass  # Best-effort — history is advisory, not blocking


def _validate_retry_progress(project_dir, step, gate):
    """Check circuit breaker state based on retry pACS score history.

    P1 Compliance: Deterministic JSONL parse + arithmetic.
    SOT Compliance: Read-only — reads {gate}-logs/ history file only.

    Checks:
      RP1: History file parse (JSONL lines, malformed lines skipped)
      RP2: Consecutive pACS delta calculation (pure arithmetic)
      RP3: NO_PROGRESS_WINDOW consecutive delta <= NO_PROGRESS_THRESHOLD
           → circuit_breaker = "OPEN"

    Args:
        project_dir: Project root directory
        step: Step number
        gate: Quality gate type

    Returns:
        dict: {
            "circuit_breaker": "OPEN" | "CLOSED",
            "history_length": int,
            "recent_scores": list[int],
            "recent_deltas": list[int]
        }
    """
    result = {
        "circuit_breaker": "CLOSED",
        "history_length": 0,
        "recent_scores": [],
        "recent_deltas": [],
    }

    path = _history_path(project_dir, step, gate)
    if not os.path.exists(path):
        return result

    # RP1: Parse JSONL history (skip malformed lines)
    scores = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    score = entry.get("pacs_score")
                    if isinstance(score, (int, float)):
                        scores.append(int(score))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue  # Skip malformed lines
    except IOError:
        return result

    result["history_length"] = len(scores)
    if len(scores) < 2:
        # Need at least 2 scores to compute deltas
        result["recent_scores"] = scores[-NO_PROGRESS_WINDOW:] if scores else []
        return result

    # RP2: Calculate consecutive deltas (score[i] - score[i-1])
    deltas = []
    for i in range(1, len(scores)):
        deltas.append(scores[i] - scores[i - 1])

    # RP3: Check last NO_PROGRESS_WINDOW deltas for stagnation
    recent_deltas = deltas[-NO_PROGRESS_WINDOW:]
    recent_scores = scores[-(NO_PROGRESS_WINDOW + 1):]

    result["recent_scores"] = recent_scores
    result["recent_deltas"] = recent_deltas

    if len(recent_deltas) >= NO_PROGRESS_WINDOW:
        all_no_progress = all(
            d <= NO_PROGRESS_THRESHOLD for d in recent_deltas
        )
        if all_no_progress:
            result["circuit_breaker"] = "OPEN"

    return result


def main():
    parser = argparse.ArgumentParser(
        description="P1 Validation for retry budget (ULW-aware)"
    )
    parser.add_argument("--step", type=int, required=True, help="Step number")
    parser.add_argument(
        "--gate",
        choices=VALID_GATES,
        required=True,
        help="Quality gate type (verification, pacs, review)",
    )
    parser.add_argument(
        "--project-dir", default=".", help="Project root directory"
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--check-and-increment",
        action="store_true",
        help="Atomic: check budget, increment if allowed (RECOMMENDED)",
    )
    mode_group.add_argument(
        "--increment",
        action="store_true",
        help="Unconditional increment (legacy — prefer --check-and-increment)",
    )
    mode_group.add_argument(
        "--check-progress",
        action="store_true",
        help="Check retry progress circuit breaker (read-only)",
    )
    mode_group.add_argument(
        "--record-attempt",
        action="store_true",
        help="Record a retry attempt with pACS score to history file",
    )

    # --record-attempt dependent arguments (outside mode_group)
    parser.add_argument(
        "--pacs-score", type=int,
        help="pACS score for --record-attempt (0-100)",
    )
    parser.add_argument(
        "--weak-dimension", type=str, choices=["F", "C", "L"],
        help="Weakest dimension for --record-attempt",
    )
    parser.add_argument(
        "--gate-result", type=str, default="FAIL",
        help="Gate result for --record-attempt (default: FAIL)",
    )

    args = parser.parse_args()

    # Argument validation: --record-attempt requires --pacs-score
    if args.record_attempt and args.pacs_score is None:
        parser.error("--record-attempt requires --pacs-score")
    project_dir = os.path.abspath(args.project_dir)

    # RB2: Detect ULW state
    ulw_active = _detect_ulw_from_snapshot(project_dir)
    max_retries = ULW_MAX_RETRIES if ulw_active else DEFAULT_MAX_RETRIES

    # Counter file path
    counter_file = _counter_path(project_dir, args.step, args.gate)

    # --- New modes: check-progress and record-attempt (independent of budget) ---
    if args.check_progress:
        progress = _validate_retry_progress(project_dir, args.step, args.gate)
        result = {
            "valid": True,
            "step": args.step,
            "gate": args.gate,
            **progress,
        }
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.record_attempt:
        _record_retry_attempt(
            project_dir, args.step, args.gate,
            args.pacs_score, args.weak_dimension, args.gate_result,
        )
        result = {
            "valid": True,
            "recorded": True,
            "step": args.step,
            "gate": args.gate,
            "pacs_score": args.pacs_score,
        }
        if args.weak_dimension:
            result["weak_dimension"] = args.weak_dimension
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    # --- Original modes: budget check / check-and-increment / increment ---
    # Determine mode and execute
    incremented = False
    reason = None  # Why can_retry is false (budget_exhausted | circuit_breaker_open)
    cb_state = None  # Circuit breaker state (included in output when checked)

    if args.check_and_increment:
        # Atomic check+consume: read → compare → increment only if budget allows
        # P1: Also checks circuit breaker — LLM cannot bypass progress check
        retries_used = _read_counter(counter_file)
        can_retry = retries_used < max_retries
        if not can_retry:
            reason = "budget_exhausted"
        else:
            # P1: Circuit breaker check integrated — single call enforces both
            progress = _validate_retry_progress(
                project_dir, args.step, args.gate
            )
            cb_state = progress
            if progress["circuit_breaker"] == "OPEN":
                can_retry = False
                reason = "circuit_breaker_open"
                # Counter NOT incremented — budget preserved for after user intervention
            else:
                retries_used = _increment_counter(counter_file)
                incremented = True
    elif args.increment:
        # Legacy: unconditional increment
        retries_used = _increment_counter(counter_file)
        can_retry = retries_used < max_retries
        if not can_retry:
            reason = "budget_exhausted"
        incremented = True
    else:
        # Read-only check (also includes circuit breaker state for completeness)
        retries_used = _read_counter(counter_file)
        can_retry = retries_used < max_retries
        if not can_retry:
            reason = "budget_exhausted"
        # Include circuit breaker state in read-only mode too
        progress = _validate_retry_progress(
            project_dir, args.step, args.gate
        )
        cb_state = progress
        if can_retry and progress["circuit_breaker"] == "OPEN":
            # Read-only mode: report but don't change can_retry
            # (only --check-and-increment enforces the block)
            pass

    budget_remaining = max(0, max_retries - retries_used)

    # Build result
    checks = {
        "RB1_counter_read": "PASS",
        "RB2_ulw_detection": "PASS",
        "RB3_budget_remaining": "PASS" if reason != "budget_exhausted" else "FAIL",
    }

    result = {
        "valid": True,
        "can_retry": can_retry,
        "retries_used": retries_used,
        "max_retries": max_retries,
        "budget_remaining": budget_remaining,
        "ulw_active": ulw_active,
        "gate": args.gate,
        "step": args.step,
        "incremented": incremented,
        "checks": checks,
    }

    # Add reason when can_retry is false
    if reason:
        result["reason"] = reason

    # Add circuit breaker state when checked
    if cb_state is not None:
        result["circuit_breaker"] = cb_state["circuit_breaker"]
        if cb_state.get("recent_scores"):
            result["recent_scores"] = cb_state["recent_scores"]
        if cb_state.get("recent_deltas"):
            result["recent_deltas"] = cb_state["recent_deltas"]

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"valid": False, "error": str(e)}))
        sys.exit(1)
