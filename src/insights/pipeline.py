"""Insight Pipeline Orchestrator — Sequential execution of 7 insight modules.

Reads Stage 1-4 outputs across a multi-date window to produce structural
insights. Each module (M1-M7) is independent except M7 which depends on M1-M6.

Key design decisions:
    - Sequential execution: one module at a time for memory management
    - gc.collect() between modules (same pattern as analysis/pipeline.py)
    - Module skip on insufficient data (not fatal — other modules continue)
    - P1 validation after each module
    - Runtime SOT: data/insights/insight_state.json

CLI: ``python main.py --mode insight --window 30 --end-date 2026-04-05``

Reference: research/bigdata-insight-workflow-design.md
"""

from __future__ import annotations

import gc
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.config.constants import (
    DATA_DIR,
    DATA_INSIGHTS_DIR,
    INSIGHT_DEFAULT_WINDOW_DAYS,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)
from src.insights.validators import validate_insight_metrics, validate_module_output
from src.insights.window_assembler import WindowCorpus, assemble_window

logger = logging.getLogger(__name__)

# Module registry: name → (import_path, expected_output_files)
MODULE_REGISTRY = {
    "m1_crosslingual": {
        "entry": "src.insights.m1_crosslingual",
        "func": "run_crosslingual_analysis",
        "output_files": [
            "asymmetry_index.parquet",
            "attention_gaps.parquet",
            "sentiment_divergence.parquet",
            "filter_bubble.parquet",
        ],
        "min_days": 7,
    },
    "m2_narrative": {
        "entry": "src.insights.m2_narrative",
        "func": "run_narrative_analysis",
        "output_files": [
            "frame_evolution.parquet",
            "voice_dominance.parquet",
            "media_health.parquet",
            "info_flow_graph.json",
            "source_credibility.parquet",
        ],
        "min_days": 14,
    },
    "m3_entity": {
        "entry": "src.insights.m3_entity",
        "func": "run_entity_analysis",
        "output_files": [
            "trajectories.parquet",
            "hidden_connections.parquet",
        ],
        "min_days": 14,
    },
    "m4_temporal": {
        "entry": "src.insights.m4_temporal",
        "func": "run_temporal_analysis",
        "output_files": [
            "velocity_map.parquet",
            "decay_curves.parquet",
        ],
        "min_days": 14,
    },
    "m5_geopolitical": {
        "entry": "src.insights.m5_geopolitical",
        "func": "run_geopolitical_analysis",
        "output_files": [
            "bilateral_index.parquet",
            "soft_power.parquet",
            "agenda_influence.parquet",
        ],
        "min_days": 14,
    },
    "m6_economic": {
        "entry": "src.insights.m6_economic",
        "func": "run_economic_analysis",
        "output_files": [
            "epu_index.parquet",
            "sector_sentiment.parquet",
            "narrative_economics.parquet",
            "hype_cycle.parquet",
        ],
        "min_days": 7,
    },
    "m7_synthesis": {
        "entry": "src.insights.m7_synthesis",
        "func": "run_synthesis",
        "output_files": [
            "insight_report.md",
            "insight_data.json",
            "key_findings.json",
        ],
        "min_days": 7,
        "depends_on": ["m1_crosslingual", "m2_narrative", "m3_entity",
                       "m4_temporal", "m5_geopolitical", "m6_economic"],
    },
}

# Modules in execution order (M1-M6 independent, M7 last)
EXECUTION_ORDER = [
    "m1_crosslingual", "m2_narrative", "m3_entity",
    "m4_temporal", "m5_geopolitical", "m6_economic",
    "m7_synthesis",
]


@dataclass
class ModuleResult:
    """Result from a single insight module."""

    name: str
    success: bool
    elapsed_seconds: float = 0.0
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class InsightPipelineResult:
    """Aggregate result from the full insight pipeline."""

    success: bool
    modules_completed: list[str] = field(default_factory=list)
    modules_failed: list[str] = field(default_factory=list)
    modules_skipped: list[str] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    window_days: int = 0
    end_date: str = ""
    output_dir: str = ""
    validation_passed: bool = False
    validation_errors: list[str] = field(default_factory=list)
    module_results: dict[str, ModuleResult] = field(default_factory=dict)


class InsightPipeline:
    """Orchestrates the 7-module insight pipeline.

    Args:
        end_date: Last date in the analysis window.
        window_days: Number of days in the window.
        module: Optional specific module to run (None = all).
        data_dir: Root data directory.
    """

    def __init__(
        self,
        end_date: date,
        window_days: int = INSIGHT_DEFAULT_WINDOW_DAYS,
        module: Optional[str] = None,
        data_dir: Optional[Path] = None,
    ):
        self.end_date = end_date
        self.window_days = window_days
        self.target_module = module
        self.data_dir = data_dir or DATA_DIR

        # Determine run ID for output directory
        if window_days <= 7:
            iso_year, iso_week, _ = end_date.isocalendar()
            self.run_id = f"weekly-{iso_year}-W{iso_week:02d}"
        elif window_days <= 31:
            self.run_id = f"monthly-{end_date.strftime('%Y-%m')}"
        else:
            quarter = (end_date.month - 1) // 3 + 1
            self.run_id = f"quarterly-{end_date.year}-Q{quarter}"

        self.output_dir = DATA_INSIGHTS_DIR / self.run_id

    def run(self) -> InsightPipelineResult:
        """Execute the insight pipeline.

        Returns:
            InsightPipelineResult with per-module outcomes.
        """
        t0 = time.monotonic()
        result = InsightPipelineResult(
            success=False,
            window_days=self.window_days,
            end_date=self.end_date.strftime("%Y-%m-%d"),
            output_dir=str(self.output_dir),
        )

        # --- Phase 0: Window Assembly ---
        logger.info(
            "=== Insight Pipeline: window=%d days, end=%s, run_id=%s ===",
            self.window_days, self.end_date, self.run_id,
        )

        try:
            corpus, availability = assemble_window(
                self.data_dir, self.end_date, self.window_days
            )
        except ValueError as e:
            logger.error("Window assembly failed: %s", e)
            result.total_elapsed_seconds = time.monotonic() - t0
            return result

        # Create output directory structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Determine modules to run
        if self.target_module:
            if self.target_module not in MODULE_REGISTRY:
                logger.error("Unknown module: %s", self.target_module)
                result.total_elapsed_seconds = time.monotonic() - t0
                return result
            modules_to_run = [self.target_module]
        else:
            modules_to_run = EXECUTION_ORDER

        # --- Execute modules sequentially ---
        all_metrics: dict[str, Any] = {}

        for module_name in modules_to_run:
            module_info = MODULE_REGISTRY[module_name]

            # Check minimum days requirement
            if corpus.total_available_days < module_info["min_days"]:
                skip_reason = (
                    f"Insufficient data: {corpus.total_available_days} days "
                    f"< {module_info['min_days']} required"
                )
                logger.warning("Skipping %s: %s", module_name, skip_reason)
                mod_result = ModuleResult(
                    name=module_name, success=False,
                    skipped=True, skip_reason=skip_reason,
                )
                result.modules_skipped.append(module_name)
                result.module_results[module_name] = mod_result
                continue

            # Check dependencies (M7 depends on M1-M6)
            deps = module_info.get("depends_on", [])
            unmet_deps = [d for d in deps if d not in result.modules_completed]
            if unmet_deps:
                # M7 can still run with partial results — just warn
                logger.warning(
                    "%s: dependencies not fully met: %s (continuing with partial data)",
                    module_name, unmet_deps,
                )

            # Execute module
            mod_result = self._run_module(module_name, module_info, corpus, all_metrics)
            result.module_results[module_name] = mod_result

            if mod_result.success:
                result.modules_completed.append(module_name)
                all_metrics[module_name.replace("m1_", "").replace("m2_", "")
                            .replace("m3_", "").replace("m4_", "")
                            .replace("m5_", "").replace("m6_", "")
                            .replace("m7_", "")] = mod_result.metrics
            elif mod_result.skipped:
                result.modules_skipped.append(module_name)
            else:
                result.modules_failed.append(module_name)

            # Memory cleanup between modules
            gc.collect()

        # --- P1 Validation ---
        valid, val_errors = validate_insight_metrics(all_metrics)
        result.validation_passed = valid
        result.validation_errors = val_errors

        # --- Write runtime SOT ---
        result.success = len(result.modules_completed) > 0
        result.total_elapsed_seconds = time.monotonic() - t0
        self._write_insight_state(result, availability)

        # --- Summary ---
        logger.info(
            "=== Insight Pipeline complete: %d/%d modules, %.1fs, validation=%s ===",
            len(result.modules_completed),
            len(modules_to_run),
            result.total_elapsed_seconds,
            "PASS" if valid else f"FAIL ({len(val_errors)} errors)",
        )

        return result

    def _run_module(
        self,
        module_name: str,
        module_info: dict,
        corpus: WindowCorpus,
        prior_metrics: dict[str, Any],
    ) -> ModuleResult:
        """Execute a single insight module.

        Dynamically imports the module and calls its run function.
        """
        t0 = time.monotonic()
        logger.info("--- Running %s ---", module_name)

        # Create module output directory
        # Map module name to subdirectory: m1_crosslingual → crosslingual
        subdir = module_name.split("_", 1)[1] if "_" in module_name else module_name
        module_output_dir = self.output_dir / subdir
        module_output_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Dynamic import
            import importlib
            mod = importlib.import_module(module_info["entry"])
            run_func = getattr(mod, module_info["func"])

            # Call module's run function with standardized interface
            metrics = run_func(
                corpus=corpus,
                output_dir=module_output_dir,
                prior_metrics=prior_metrics,
            )

            elapsed = time.monotonic() - t0

            # Validate output files exist (L0)
            valid, file_errors = validate_module_output(
                module_output_dir,
                module_name,
                module_info["output_files"],
            )

            if not valid:
                logger.warning(
                    "%s: output validation failed: %s",
                    module_name, file_errors,
                )

            logger.info(
                "%s completed: %.1fs, %d metrics, output_valid=%s",
                module_name, elapsed, len(metrics), valid,
            )

            return ModuleResult(
                name=module_name,
                success=True,
                elapsed_seconds=elapsed,
                metrics=metrics,
                errors=file_errors,
            )

        except Exception as e:
            elapsed = time.monotonic() - t0
            logger.error("%s failed: %s", module_name, e, exc_info=True)
            return ModuleResult(
                name=module_name,
                success=False,
                elapsed_seconds=elapsed,
                errors=[str(e)],
            )

    def _write_insight_state(
        self,
        result: InsightPipelineResult,
        availability,
    ) -> None:
        """Write runtime SOT for the insight pipeline.

        File: data/insights/insight_state.json
        This is the runtime SOT for Workflow B (separate from build SOT .claude/state.yaml).
        """
        state = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "last_window": self.window_days,
            "last_end_date": self.end_date.strftime("%Y-%m-%d"),
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "modules_completed": result.modules_completed,
            "modules_failed": result.modules_failed,
            "modules_skipped": result.modules_skipped,
            "total_elapsed_seconds": round(result.total_elapsed_seconds, 1),
            "validation_passed": result.validation_passed,
            "validation_errors_count": len(result.validation_errors),
            "data_coverage": {
                "available_days": len(availability.available_dates),
                "window_days": availability.window_days,
                "coverage_ratio": round(availability.coverage_ratio, 3),
                "missing_dates": availability.missing_dates[:10],
            },
        }

        state_path = DATA_INSIGHTS_DIR / "insight_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        with open(state_path, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)

        logger.info("Runtime SOT written: %s", state_path)


def run_insight_pipeline(
    end_date: date,
    window_days: int = INSIGHT_DEFAULT_WINDOW_DAYS,
    module: Optional[str] = None,
) -> InsightPipelineResult:
    """Convenience function for CLI integration.

    Args:
        end_date: Last date in the analysis window.
        window_days: Number of days in the window.
        module: Optional specific module to run.

    Returns:
        InsightPipelineResult.
    """
    pipeline = InsightPipeline(
        end_date=end_date,
        window_days=window_days,
        module=module,
    )
    return pipeline.run()
