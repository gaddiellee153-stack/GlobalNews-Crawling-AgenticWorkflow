"""Window Assembler — Load and merge multi-date Parquet files for insight analysis.

Reads Stage 1-4 outputs across a date window (e.g., 30 days) and provides
lazy-loaded, column-selective access to the merged corpus.

Key design decisions:
    - Lazy loading: PyArrow memory-mapped reads, no full materialization
    - Column-selective: each module requests only the columns it needs
    - Gap handling: missing/incomplete dates are tracked, not fatal
    - P1 validation: validate_window_availability() ensures minimum coverage

Reference: research/bigdata-insight-workflow-design.md, Gap 2 (Reflection #3).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

from src.config.constants import (
    DATA_ANALYSIS_DIR,
    DATA_FEATURES_DIR,
    DATA_PROCESSED_DIR,
    INSIGHT_MIN_COVERAGE_RATIO,
)

logger = logging.getLogger(__name__)


# Stage 1-4 output files that Workflow B consumes (READ-ONLY)
REQUIRED_DAILY_FILES = {
    "articles": "processed/{date}/articles.parquet",
    "embeddings": "features/{date}/embeddings.parquet",
    "ner": "features/{date}/ner.parquet",
    "tfidf": "features/{date}/tfidf.parquet",
    "article_analysis": "analysis/{date}/article_analysis.parquet",
    "topics": "analysis/{date}/topics.parquet",
    "networks": "analysis/{date}/networks.parquet",
}

# Minimum required files per date (articles + article_analysis are essential)
ESSENTIAL_FILES = {"articles", "article_analysis"}


@dataclass
class WindowAvailability:
    """Result of window availability validation."""

    available_dates: list[str] = field(default_factory=list)
    missing_dates: list[str] = field(default_factory=list)
    incomplete_dates: list[str] = field(default_factory=list)
    window_days: int = 0
    coverage_ratio: float = 0.0
    sufficient: bool = False
    per_date_files: dict[str, list[str]] = field(default_factory=dict)


def validate_window_availability(
    data_dir: Path,
    end_date: date,
    window_days: int,
    min_coverage: float = INSIGHT_MIN_COVERAGE_RATIO,
) -> WindowAvailability:
    """P1 deterministic: validate data availability within the analysis window.

    Checks that Stage 1-4 outputs exist for a sufficient fraction of dates
    in the window. Does NOT load data — only checks file existence.

    Args:
        data_dir: Root data directory (e.g., Path("data/")).
        end_date: Last date in the window (inclusive).
        window_days: Number of days in the window.
        min_coverage: Minimum fraction of dates with data (default 0.7).

    Returns:
        WindowAvailability with per-date file inventory.
    """
    result = WindowAvailability(window_days=window_days)

    for day_offset in range(window_days):
        target = end_date - timedelta(days=day_offset)
        date_str = target.strftime("%Y-%m-%d")

        existing_files = []
        for key, path_template in REQUIRED_DAILY_FILES.items():
            fpath = data_dir / path_template.format(date=date_str)
            if fpath.exists() and fpath.stat().st_size > 0:
                existing_files.append(key)

        result.per_date_files[date_str] = existing_files

        essential_present = ESSENTIAL_FILES.issubset(set(existing_files))
        if essential_present and len(existing_files) >= len(REQUIRED_DAILY_FILES) - 1:
            result.available_dates.append(date_str)
        elif len(existing_files) > 0:
            result.incomplete_dates.append(date_str)
        else:
            result.missing_dates.append(date_str)

    result.coverage_ratio = (
        len(result.available_dates) / window_days if window_days > 0 else 0.0
    )
    result.sufficient = result.coverage_ratio >= min_coverage

    logger.info(
        "Window availability: %d/%d days (%.1f%%), sufficient=%s, "
        "missing=%d, incomplete=%d",
        len(result.available_dates),
        window_days,
        result.coverage_ratio * 100,
        result.sufficient,
        len(result.missing_dates),
        len(result.incomplete_dates),
    )

    return result


@dataclass
class WindowCorpus:
    """Provides access to the merged multi-date corpus.

    Does NOT hold data in memory — provides load_*() methods that
    return pandas DataFrames with column selection and date filtering.
    """

    data_dir: Path
    available_dates: list[str]
    end_date: str
    window_days: int

    def load_parquet(
        self,
        file_key: str,
        columns: Optional[list[str]] = None,
        dates: Optional[list[str]] = None,
    ):
        """Load and concatenate Parquet files across dates.

        IMPORTANT: ``_crawl_date`` is a virtual column added AFTER loading.
        Do NOT include it in the ``columns`` parameter — PyArrow will raise
        KeyError because it does not exist in the source Parquet file.
        It is always available in the returned DataFrame regardless of
        the ``columns`` filter.

        Args:
            file_key: One of REQUIRED_DAILY_FILES keys (e.g., "articles").
            columns: Columns to read from Parquet (None = all).
                     Do NOT include "_crawl_date" here.
            dates: Specific dates to load (None = all available dates).

        Returns:
            pandas DataFrame with data from all requested dates.
            Always includes ``_crawl_date`` column (added after load).
        """
        import pandas as pd

        target_dates = dates or self.available_dates
        path_template = REQUIRED_DAILY_FILES[file_key]
        frames = []

        for date_str in sorted(target_dates):
            fpath = self.data_dir / path_template.format(date=date_str)
            if not fpath.exists():
                logger.debug("Skipping missing file: %s", fpath)
                continue
            try:
                # Filter out _crawl_date from requested columns — it's added after read
                read_cols = columns
                if columns and "_crawl_date" in columns:
                    read_cols = [c for c in columns if c != "_crawl_date"]
                df = pq.read_table(
                    fpath, columns=read_cols if read_cols else None
                ).to_pandas()
                # Add crawl_date column for multi-date tracking
                df["_crawl_date"] = date_str
                frames.append(df)
            except Exception as e:
                logger.warning("Failed to read %s: %s", fpath, e)
                continue

        if not frames:
            logger.warning("No data loaded for file_key=%s", file_key)
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        logger.info(
            "Loaded %s: %d rows from %d dates (columns=%s)",
            file_key,
            len(merged),
            len(frames),
            columns or "all",
        )
        return merged

    @property
    def total_available_days(self) -> int:
        return len(self.available_dates)


def assemble_window(
    data_dir: Path,
    end_date: date,
    window_days: int,
) -> tuple[WindowCorpus, WindowAvailability]:
    """Validate and prepare a WindowCorpus for insight analysis.

    Args:
        data_dir: Root data directory.
        end_date: Last date in the window.
        window_days: Number of days in the window.

    Returns:
        Tuple of (WindowCorpus, WindowAvailability).

    Raises:
        ValueError: If minimum coverage is not met.
    """
    availability = validate_window_availability(data_dir, end_date, window_days)

    if not availability.sufficient:
        raise ValueError(
            f"Insufficient data coverage: {availability.coverage_ratio:.1%} "
            f"({len(availability.available_dates)}/{window_days} days). "
            f"Minimum required: {INSIGHT_MIN_COVERAGE_RATIO:.0%}. "
            f"Missing dates: {availability.missing_dates[:5]}..."
        )

    corpus = WindowCorpus(
        data_dir=data_dir,
        available_dates=availability.available_dates,
        end_date=end_date.strftime("%Y-%m-%d"),
        window_days=window_days,
    )

    return corpus, availability
