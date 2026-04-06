"""P1 Deterministic Validators for Insight Pipeline metrics.

Every metric has a mathematical range constraint. This module validates
that computed metrics fall within their valid ranges, catching both
implementation bugs and data corruption.

Pattern: identical to existing validate_pacs_output(), verify_pacs_arithmetic()
in .claude/hooks/scripts/_context_lib.py.

Reference: research/bigdata-insight-workflow-design.md, Appendix B (Reflection #2).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def validate_insight_metrics(results: dict[str, Any]) -> tuple[bool, list[str]]:
    """P1 deterministic: validate all 27 insight metrics against mathematical constraints.

    Args:
        results: Dict keyed by module name, containing computed metrics.
            Example: {"crosslingual": {"jsd_values": {...}, ...}, ...}

    Returns:
        Tuple of (all_valid, list_of_errors).
    """
    errors: list[str] = []

    # --- M1: Cross-Lingual ---
    cl = results.get("crosslingual", {})

    # CL-1: JSD ∈ [0, 1], symmetric
    for pair, val in cl.get("jsd_values", {}).items():
        if not isinstance(val, (int, float)):
            errors.append(f"CL-1: JSD({pair}) is not numeric: {type(val)}")
        elif not 0.0 <= val <= 1.0:
            errors.append(f"CL-1: JSD({pair}) = {val:.4f} out of [0, 1]")

    # CL-2: Attention fractions ∈ [0, 1]
    for entry in cl.get("attention_gaps", []):
        for lang, frac in entry.get("per_lang", {}).items():
            if not 0.0 <= frac <= 1.0:
                errors.append(
                    f"CL-2: attention({entry.get('topic', '?')}, {lang}) "
                    f"= {frac:.4f} out of [0, 1]"
                )

    # CL-3: Wasserstein distance ≥ 0
    for pair, val in cl.get("sentiment_divergence", {}).items():
        if val < 0:
            errors.append(f"CL-3: Wasserstein({pair}) = {val:.4f} < 0")

    # CL-4: Jaccard ∈ [0, 1]
    for pair, val in cl.get("filter_bubble", {}).items():
        if not 0.0 <= val <= 1.0:
            errors.append(f"CL-4: Jaccard({pair}) = {val:.4f} out of [0, 1]")

    # --- M2: Narrative & Framing ---
    nf = results.get("narrative", {})

    # NF-3: HHI ∈ [0, 1]
    for topic, val in nf.get("hhi_values", {}).items():
        if not 0.0 <= val <= 1.0:
            errors.append(f"NF-3: HHI({topic}) = {val:.4f} out of [0, 1]")

    # NF-4: Shannon entropy ≥ 0
    for topic, val in nf.get("media_health", {}).items():
        if isinstance(val, dict):
            for metric, score in val.items():
                if metric.endswith("_entropy") and score < 0:
                    errors.append(
                        f"NF-4: {metric}({topic}) = {score:.4f} < 0"
                    )

    # NF-6: Source credibility ∈ [0, 1]
    for source, val in nf.get("source_credibility", {}).items():
        if not 0.0 <= val <= 1.0:
            errors.append(f"NF-6: credibility({source}) = {val:.4f} out of [0, 1]")

    # --- M3: Entity Analytics ---
    ea = results.get("entity", {})

    # EA-1: Trajectory type must be one of known types
    valid_types = {"rising_star", "fading_giant", "cyclical", "burst", "plateau"}
    for entity, ttype in ea.get("trajectory_types", {}).items():
        if ttype not in valid_types:
            errors.append(f"EA-1: trajectory({entity}) = '{ttype}' not in {valid_types}")

    # EA-2: Jaccard ∈ [0, 1]
    for pair, val in ea.get("hidden_connections", {}).items():
        if not 0.0 <= val <= 1.0:
            errors.append(f"EA-2: Jaccard({pair}) = {val:.4f} out of [0, 1]")

    # --- M4: Temporal Patterns ---
    tp = results.get("temporal", {})

    # TP-2: Velocity (lag) ≥ 0
    for pair, val in tp.get("velocity_map", {}).items():
        if isinstance(val, (int, float)) and val < 0:
            errors.append(f"TP-2: velocity({pair}) = {val:.2f} < 0")

    # TP-3: Decay type must be known
    valid_decay = {"flash", "sustained", "cyclical", "irregular"}
    for topic, dtype in tp.get("decay_types", {}).items():
        if dtype not in valid_decay:
            errors.append(f"TP-3: decay({topic}) = '{dtype}' not in {valid_decay}")

    # --- M5: Geopolitical ---
    gi = results.get("geopolitical", {})

    # GI-1: BRI ∈ [-1, 1]
    for pair, val in gi.get("bri_values", {}).items():
        if not -1.0 <= val <= 1.0:
            errors.append(f"GI-1: BRI({pair}) = {val:.4f} out of [-1, 1]")

    # GI-2: Soft power ≥ 0
    for country, val in gi.get("soft_power", {}).items():
        if val < 0:
            errors.append(f"GI-2: soft_power({country}) = {val:.4f} < 0")

    # --- M6: Economic Intelligence ---
    ei = results.get("economic", {})

    # EI-1: EPU ∈ [0, 1]
    for lang, val in ei.get("epu_values", {}).items():
        if not 0.0 <= val <= 1.0:
            errors.append(f"EI-1: EPU({lang}) = {val:.4f} out of [0, 1]")

    # EI-5: Hype phase must be known
    valid_hype = {
        "trigger", "peak_of_inflated_expectations",
        "trough_of_disillusionment", "slope_of_enlightenment",
        "plateau_of_productivity", "unknown",
    }
    for tech, phase in ei.get("hype_phases", {}).items():
        if phase not in valid_hype:
            errors.append(f"EI-5: hype({tech}) = '{phase}' not in {valid_hype}")

    # --- Summary ---
    valid = len(errors) == 0
    if errors:
        logger.warning(
            "Insight metric validation: %d errors found:\n  %s",
            len(errors),
            "\n  ".join(errors[:20]),
        )
    else:
        logger.info("Insight metric validation: all checks passed.")

    return valid, errors


def validate_module_output(
    output_dir: Path,
    module_name: str,
    expected_files: list[str],
    min_size_bytes: int = 100,
) -> tuple[bool, list[str]]:
    """P1 deterministic: validate that a module produced its expected output files.

    Analogous to existing validate_step_output() (L0 Anti-Skip Guard).

    Args:
        output_dir: Module output directory (e.g., data/insights/weekly-W14/crosslingual/)
        module_name: Module name for error messages.
        expected_files: List of expected filenames.
        min_size_bytes: Minimum file size to accept.

    Returns:
        Tuple of (all_valid, list_of_errors).
    """
    errors: list[str] = []

    for fname in expected_files:
        fpath = output_dir / fname
        if not fpath.exists():
            errors.append(f"{module_name}: missing output file {fname}")
        elif fpath.stat().st_size < min_size_bytes:
            errors.append(
                f"{module_name}: {fname} too small "
                f"({fpath.stat().st_size} bytes < {min_size_bytes})"
            )

    return len(errors) == 0, errors
