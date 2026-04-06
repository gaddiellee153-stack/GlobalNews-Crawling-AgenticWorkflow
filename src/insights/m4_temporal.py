"""M4: Temporal Pattern Analysis — Event cascades, information velocity,
attention decay, and structural cyclicality.

4 metrics (TP-1 through TP-4), all Type A (pure arithmetic).
NO LLM calls. NO ML model inference.

TP-1: Event Cascade Detection (Granger causality)
TP-2: Information Velocity Matrix (cross-lingual first-appearance lag)
TP-3: Attention Decay Classification (exponential vs power-law fit)
TP-4: Structural Cyclicality Detection (FFT periodogram)

Input:  Stage 1-4 Parquet files via WindowCorpus (READ-ONLY)
Output: velocity_map.parquet, decay_curves.parquet + metrics dict

Reference: research/bigdata-insight-workflow-design.md, M4 specification.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.optimize import curve_fit

from src.config.constants import PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL
from src.insights.constants import (
    DECAY_FLASH_LAMBDA_MIN,
    DECAY_R2_THRESHOLD,
    DECAY_SUSTAINED_ALPHA_MAX,
    FFT_MIN_POWER_RATIO,
    HAWKES_MIN_EVENTS,
    VELOCITY_MAX_LAG_HOURS,
)
from src.insights.window_assembler import WindowCorpus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

# Minimum window days for Granger causality (TP-1) and FFT (TP-4)
_MIN_WINDOW_DAYS_GRANGER = 14
_GRANGER_MAX_LAG = 7
_GRANGER_P_THRESHOLD = 0.05
_GRANGER_TOP_TOPICS = 10

# Minimum post-peak data points for decay fitting (TP-3)
_DECAY_MIN_POST_PEAK_POINTS = 5

# Minimum days for FFT analysis (TP-4)
_FFT_MIN_DAYS = 14


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to Parquet with project-standard ZSTD compression."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table,
        str(path),
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )
    logger.info(
        "Parquet written: %s (%d rows, %d cols)",
        path.name, len(df), len(df.columns),
    )


def _build_daily_topic_series(
    topics_df: pd.DataFrame,
    available_dates: list[str],
) -> dict[str, pd.Series]:
    """Build daily article count time series per topic.

    Args:
        topics_df: DataFrame with [topic_id, _crawl_date].
        available_dates: Sorted date strings covering the window.

    Returns:
        Dict mapping topic_id -> pd.Series indexed by date string,
        with article counts (0-filled for missing dates).
    """
    if topics_df.empty:
        return {}

    counts = (
        topics_df.groupby(["topic_id", "_crawl_date"])
        .size()
        .reset_index(name="count")
    )

    date_index = sorted(available_dates)
    series_map: dict[str, pd.Series] = {}

    for topic_id, group in counts.groupby("topic_id"):
        day_counts = group.set_index("_crawl_date")["count"]
        # Reindex to full date range, fill missing with 0
        day_counts = day_counts.reindex(date_index, fill_value=0)
        series_map[str(topic_id)] = day_counts

    return series_map


# ---------------------------------------------------------------------------
# TP-1: Event Cascade Detection (Granger Causality)
# ---------------------------------------------------------------------------

def _compute_cascades(
    topics_df: pd.DataFrame,
    available_dates: list[str],
    window_days: int,
) -> pd.DataFrame:
    """Detect which topics Granger-cause other topics.

    Uses statsmodels.tsa.stattools.grangercausalitytests on daily volume
    time series. Top 10 topics by article count are tested pairwise.

    Args:
        topics_df: DataFrame with [topic_id, topic_label, _crawl_date].
        available_dates: Sorted date strings.
        window_days: Length of the analysis window in days.

    Returns:
        DataFrame with columns:
            analysis_type, cause_topic, effect_topic, lag, p_value
    """
    if window_days < _MIN_WINDOW_DAYS_GRANGER:
        logger.warning(
            "TP-1: window (%d days) < %d minimum for Granger causality, skipping",
            window_days, _MIN_WINDOW_DAYS_GRANGER,
        )
        return pd.DataFrame(
            columns=["analysis_type", "cause_topic", "effect_topic", "lag", "p_value"]
        )

    # Select top N topics by article count
    topic_counts = topics_df.groupby("topic_id").size().sort_values(ascending=False)
    top_topics = topic_counts.head(_GRANGER_TOP_TOPICS).index.tolist()

    if len(top_topics) < 2:
        logger.warning("TP-1: fewer than 2 topics with data, skipping Granger")
        return pd.DataFrame(
            columns=["analysis_type", "cause_topic", "effect_topic", "lag", "p_value"]
        )

    # Build daily volume time series
    series_map = _build_daily_topic_series(topics_df, available_dates)

    # Filter to top topics with sufficient data points
    valid_topics: list[str] = []
    for tid in top_topics:
        tid_str = str(tid)
        if tid_str in series_map:
            non_zero = (series_map[tid_str] > 0).sum()
            if non_zero >= HAWKES_MIN_EVENTS:
                valid_topics.append(tid_str)

    if len(valid_topics) < 2:
        logger.warning(
            "TP-1: fewer than 2 topics with >= %d non-zero days, skipping",
            HAWKES_MIN_EVENTS,
        )
        return pd.DataFrame(
            columns=["analysis_type", "cause_topic", "effect_topic", "lag", "p_value"]
        )

    # Build topic_id -> topic_label mapping for readable output
    label_map: dict[str, str] = {}
    if "topic_label" in topics_df.columns:
        label_df = topics_df.drop_duplicates("topic_id")[["topic_id", "topic_label"]]
        label_map = dict(zip(label_df["topic_id"].astype(str), label_df["topic_label"]))

    # Import grangercausalitytests (deferred to avoid import cost if skipped)
    try:
        from statsmodels.tsa.stattools import grangercausalitytests
    except ImportError:
        logger.error("TP-1: statsmodels not available, skipping Granger causality")
        return pd.DataFrame(
            columns=["analysis_type", "cause_topic", "effect_topic", "lag", "p_value"]
        )

    rows: list[dict[str, Any]] = []
    n_data_points = len(available_dates)
    max_lag = min(_GRANGER_MAX_LAG, n_data_points // 3)

    if max_lag < 1:
        logger.warning("TP-1: insufficient data points (%d) for any lag, skipping", n_data_points)
        return pd.DataFrame(
            columns=["analysis_type", "cause_topic", "effect_topic", "lag", "p_value"]
        )

    for i, cause_tid in enumerate(valid_topics):
        for effect_tid in valid_topics[i + 1:]:
            cause_series = series_map[cause_tid].values.astype(np.float64)
            effect_series = series_map[effect_tid].values.astype(np.float64)

            # Test both directions: cause -> effect and effect -> cause
            for x_tid, y_tid, x_vals, y_vals in [
                (cause_tid, effect_tid, cause_series, effect_series),
                (effect_tid, cause_tid, effect_series, cause_series),
            ]:
                data = np.column_stack([y_vals, x_vals])

                # Variance check: Granger test requires non-constant series
                if np.std(y_vals) < 1e-10 or np.std(x_vals) < 1e-10:
                    continue

                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        gc_results = grangercausalitytests(
                            data, maxlag=max_lag, verbose=False,
                        )

                    # Find best (lowest p-value) lag
                    best_lag = 1
                    best_p = 1.0
                    for lag_val in range(1, max_lag + 1):
                        if lag_val not in gc_results:
                            continue
                        # Use the ssr_ftest p-value (most standard)
                        test_results = gc_results[lag_val][0]
                        p_val = test_results["ssr_ftest"][1]
                        if p_val < best_p:
                            best_p = p_val
                            best_lag = lag_val

                    if best_p < _GRANGER_P_THRESHOLD:
                        rows.append({
                            "analysis_type": "cascade",
                            "cause_topic": label_map.get(x_tid, x_tid),
                            "effect_topic": label_map.get(y_tid, y_tid),
                            "lag": best_lag,
                            "p_value": round(best_p, 6),
                        })

                except Exception as e:
                    logger.debug(
                        "TP-1: Granger test failed for %s -> %s: %s",
                        x_tid, y_tid, e,
                    )
                    continue

    logger.info(
        "TP-1: tested %d topic pairs, found %d significant cascade pairs (p < %.2f)",
        len(valid_topics) * (len(valid_topics) - 1),
        len(rows),
        _GRANGER_P_THRESHOLD,
    )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TP-2: Information Velocity Matrix
# ---------------------------------------------------------------------------

def _compute_velocity(
    topics_df: pd.DataFrame,
    articles_df: pd.DataFrame,
) -> pd.DataFrame:
    """Measure cross-lingual propagation speed for each topic.

    For each topic appearing in 2+ languages, find the first appearance
    (min published_at) per language. Lag = time difference in hours between
    language pairs.

    Args:
        topics_df: DataFrame with [article_id, topic_id, topic_label].
        articles_df: DataFrame with [article_id, language, published_at].

    Returns:
        DataFrame with columns:
            analysis_type, topic_id, topic_label, lang_pair,
            first_lang, second_lang, lag_hours
    """
    if topics_df.empty or articles_df.empty:
        logger.warning("TP-2: insufficient data for velocity computation")
        return pd.DataFrame(
            columns=[
                "analysis_type", "topic_id", "topic_label",
                "lang_pair", "first_lang", "second_lang", "lag_hours",
            ]
        )

    # Merge to get (topic_id, topic_label, language, published_at)
    # Only merge columns not already in topics_df to avoid suffix collisions
    merge_from_articles = ["article_id"]
    if "language" not in topics_df.columns:
        merge_from_articles.append("language")
    if "published_at" not in topics_df.columns:
        merge_from_articles.append("published_at")
    if len(merge_from_articles) > 1:
        merged = topics_df.merge(
            articles_df[merge_from_articles],
            on="article_id",
            how="inner",
        )
    else:
        merged = topics_df.copy()

    if merged.empty:
        logger.warning("TP-2: no merged topic-article data")
        return pd.DataFrame(
            columns=[
                "analysis_type", "topic_id", "topic_label",
                "lang_pair", "first_lang", "second_lang", "lag_hours",
            ]
        )

    # Ensure published_at is datetime
    merged["published_at"] = pd.to_datetime(merged["published_at"], errors="coerce")
    merged = merged.dropna(subset=["published_at"])

    if merged.empty:
        logger.warning("TP-2: no valid published_at timestamps after parsing")
        return pd.DataFrame(
            columns=[
                "analysis_type", "topic_id", "topic_label",
                "lang_pair", "first_lang", "second_lang", "lag_hours",
            ]
        )

    # Find min(published_at) per (topic_id, language)
    first_appearance = (
        merged.groupby(["topic_id", "topic_label", "language"])["published_at"]
        .min()
        .reset_index()
    )

    rows: list[dict[str, Any]] = []

    for (topic_id, topic_label), group in first_appearance.groupby(
        ["topic_id", "topic_label"]
    ):
        if len(group) < 2:
            continue

        # Sort by first appearance time
        group_sorted = group.sort_values("published_at")
        langs = group_sorted["language"].tolist()
        times = group_sorted["published_at"].tolist()

        # Compute pairwise lags between all language pairs
        for i in range(len(langs)):
            for j in range(i + 1, len(langs)):
                lag_td = times[j] - times[i]
                lag_hours = lag_td.total_seconds() / 3600.0

                if lag_hours < 0:
                    # Should not happen since sorted, but safeguard
                    continue

                if lag_hours > VELOCITY_MAX_LAG_HOURS:
                    continue

                # Canonical pair key (alphabetically)
                pair_langs = sorted([langs[i], langs[j]])
                lang_pair = f"{pair_langs[0]}-{pair_langs[1]}"

                rows.append({
                    "analysis_type": "velocity",
                    "topic_id": str(topic_id),
                    "topic_label": str(topic_label),
                    "lang_pair": lang_pair,
                    "first_lang": langs[i],
                    "second_lang": langs[j],
                    "lag_hours": round(lag_hours, 2),
                })

    if not rows:
        logger.warning("TP-2: no velocity measurements within max lag threshold")
        return pd.DataFrame(
            columns=[
                "analysis_type", "topic_id", "topic_label",
                "lang_pair", "first_lang", "second_lang", "lag_hours",
            ]
        )

    result_df = pd.DataFrame(rows)

    logger.info(
        "TP-2: computed %d velocity measurements across %d topics, %d language pairs",
        len(result_df),
        result_df["topic_id"].nunique(),
        result_df["lang_pair"].nunique(),
    )

    return result_df


# ---------------------------------------------------------------------------
# TP-3: Attention Decay Classification
# ---------------------------------------------------------------------------

def _exp_decay(t: np.ndarray, n0: float, lam: float) -> np.ndarray:
    """Exponential decay model: N(t) = N0 * exp(-lambda * t)."""
    return n0 * np.exp(-lam * t)


def _power_decay(t_log: np.ndarray, log_n0: float, alpha: float) -> np.ndarray:
    """Power-law decay in log-log space: log(N) = log(N0) - alpha * log(t)."""
    return log_n0 - alpha * t_log


def _r_squared(y_actual: np.ndarray, y_predicted: np.ndarray) -> float:
    """Compute coefficient of determination (R^2)."""
    ss_res = np.sum((y_actual - y_predicted) ** 2)
    ss_tot = np.sum((y_actual - np.mean(y_actual)) ** 2)
    if ss_tot < 1e-10:
        return 0.0
    return float(1.0 - ss_res / ss_tot)


def _classify_decay(
    topic_series: pd.Series,
    topic_id: str,
    topic_label: str,
) -> dict[str, Any] | None:
    """Classify the attention decay pattern for a single topic.

    Finds peak day, then fits exponential and power-law models to post-peak data.

    Args:
        topic_series: Daily article counts indexed by date string.
        topic_id: Topic identifier.
        topic_label: Human-readable topic label.

    Returns:
        Dict with classification results, or None if insufficient data.
    """
    values = topic_series.values.astype(np.float64)
    dates = topic_series.index.tolist()

    # Find peak day
    peak_idx = int(np.argmax(values))
    peak_count = values[peak_idx]

    if peak_count < 1:
        return None

    # Post-peak data (excluding peak day itself)
    post_peak = values[peak_idx + 1:]

    if len(post_peak) < _DECAY_MIN_POST_PEAK_POINTS:
        return None

    # Time steps (1, 2, 3, ...)
    t = np.arange(1, len(post_peak) + 1, dtype=np.float64)
    y = post_peak.copy()

    # Ensure no zeros for log transform (add small epsilon)
    y_safe = np.maximum(y, 1e-10)

    # --- Fit exponential decay: N(t) = N0 * exp(-lambda * t) ---
    r2_exp = 0.0
    lambda_val = 0.0
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            popt_exp, _ = curve_fit(
                _exp_decay, t, y,
                p0=[peak_count, 0.5],
                bounds=([0, 0], [np.inf, 10.0]),
                maxfev=5000,
            )
        y_pred_exp = _exp_decay(t, *popt_exp)
        r2_exp = _r_squared(y, y_pred_exp)
        lambda_val = float(popt_exp[1])
    except (RuntimeError, ValueError, TypeError):
        r2_exp = 0.0
        lambda_val = 0.0

    # --- Fit power-law decay: N(t) = N0 * t^(-alpha) via log-log ---
    r2_pow = 0.0
    alpha_val = 0.0
    try:
        log_t = np.log(t)
        log_y = np.log(y_safe)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            popt_pow, _ = curve_fit(
                _power_decay, log_t, log_y,
                p0=[np.log(peak_count), 0.5],
                maxfev=5000,
            )
        y_pred_log = _power_decay(log_t, *popt_pow)
        r2_pow = _r_squared(log_y, y_pred_log)
        alpha_val = float(popt_pow[1])
    except (RuntimeError, ValueError, TypeError):
        r2_pow = 0.0
        alpha_val = 0.0

    # --- Classify ---
    if r2_exp > r2_pow and r2_exp > DECAY_R2_THRESHOLD:
        decay_type = "flash"
    elif r2_pow > r2_exp and r2_pow > DECAY_R2_THRESHOLD:
        decay_type = "sustained"
    else:
        decay_type = "irregular"

    return {
        "topic_id": str(topic_id),
        "topic_label": str(topic_label),
        "peak_date": dates[peak_idx],
        "peak_count": int(peak_count),
        "post_peak_points": len(post_peak),
        "r2_exponential": round(r2_exp, 4),
        "r2_power_law": round(r2_pow, 4),
        "lambda_exp": round(lambda_val, 4),
        "alpha_pow": round(alpha_val, 4),
        "decay_type": decay_type,
    }


def _compute_decay_curves(
    topics_df: pd.DataFrame,
    available_dates: list[str],
) -> pd.DataFrame:
    """Classify attention decay for all topics.

    Args:
        topics_df: DataFrame with [topic_id, topic_label, _crawl_date].
        available_dates: Sorted date strings.

    Returns:
        DataFrame with decay classification per topic.
    """
    if len(available_dates) < 2:
        logger.warning("TP-3: single-day data, skipping decay classification")
        return pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label", "peak_date",
            "peak_count", "post_peak_points", "r2_exponential",
            "r2_power_law", "lambda_exp", "alpha_pow", "decay_type",
        ])

    # Build daily time series per topic
    series_map = _build_daily_topic_series(topics_df, available_dates)

    # Build topic_id -> topic_label mapping
    label_map: dict[str, str] = {}
    if "topic_label" in topics_df.columns:
        label_df = topics_df.drop_duplicates("topic_id")[["topic_id", "topic_label"]]
        label_map = dict(zip(label_df["topic_id"].astype(str), label_df["topic_label"]))

    rows: list[dict[str, Any]] = []

    for topic_id, series in series_map.items():
        topic_label = label_map.get(topic_id, topic_id)
        result = _classify_decay(series, topic_id, topic_label)
        if result is not None:
            result["analysis_type"] = "decay"
            rows.append(result)

    if not rows:
        logger.warning("TP-3: no topics had sufficient post-peak data for decay fitting")
        return pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label", "peak_date",
            "peak_count", "post_peak_points", "r2_exponential",
            "r2_power_law", "lambda_exp", "alpha_pow", "decay_type",
        ])

    result_df = pd.DataFrame(rows)

    type_counts = result_df["decay_type"].value_counts().to_dict()
    logger.info(
        "TP-3: classified %d topics — %s",
        len(result_df),
        ", ".join(f"{k}: {v}" for k, v in sorted(type_counts.items())),
    )

    return result_df


# ---------------------------------------------------------------------------
# TP-4: Structural Cyclicality Detection (FFT)
# ---------------------------------------------------------------------------

def _compute_cyclicality(
    topics_df: pd.DataFrame,
    available_dates: list[str],
) -> pd.DataFrame:
    """Detect weekly/monthly/annual cycles via FFT periodogram.

    For each topic, compute FFT of daily volume time series. If the
    dominant peak exceeds FFT_MIN_POWER_RATIO times the mean power,
    report the period.

    Args:
        topics_df: DataFrame with [topic_id, topic_label, _crawl_date].
        available_dates: Sorted date strings.

    Returns:
        DataFrame with columns:
            analysis_type, topic_id, topic_label, dominant_period_days,
            power_ratio, cycle_type
    """
    if len(available_dates) < _FFT_MIN_DAYS:
        logger.warning(
            "TP-4: insufficient data (%d days < %d), skipping FFT cyclicality",
            len(available_dates), _FFT_MIN_DAYS,
        )
        return pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "dominant_period_days", "power_ratio", "cycle_type",
        ])

    series_map = _build_daily_topic_series(topics_df, available_dates)

    # Build topic_id -> topic_label mapping
    label_map: dict[str, str] = {}
    if "topic_label" in topics_df.columns:
        label_df = topics_df.drop_duplicates("topic_id")[["topic_id", "topic_label"]]
        label_map = dict(zip(label_df["topic_id"].astype(str), label_df["topic_label"]))

    rows: list[dict[str, Any]] = []

    for topic_id, series in series_map.items():
        values = series.values.astype(np.float64)
        n = len(values)

        if n < _FFT_MIN_DAYS:
            continue

        # Detrend by subtracting mean
        values_detrended = values - np.mean(values)

        # Compute FFT
        fft_vals = np.fft.rfft(values_detrended)
        power = np.abs(fft_vals) ** 2
        freqs = np.fft.rfftfreq(n, d=1.0)  # d=1 day

        # Skip DC component (index 0) and Nyquist
        if len(power) < 3:
            continue

        power_no_dc = power[1:]
        freqs_no_dc = freqs[1:]

        if len(power_no_dc) == 0:
            continue

        mean_power = np.mean(power_no_dc)
        if mean_power < 1e-10:
            continue

        # Find dominant peak
        peak_idx = int(np.argmax(power_no_dc))
        peak_power = power_no_dc[peak_idx]
        peak_freq = freqs_no_dc[peak_idx]

        power_ratio = peak_power / mean_power

        if power_ratio < FFT_MIN_POWER_RATIO:
            continue

        if peak_freq < 1e-10:
            continue

        period_days = 1.0 / peak_freq

        # Classify cycle type
        if 5.0 <= period_days <= 9.0:
            cycle_type = "weekly"
        elif 25.0 <= period_days <= 35.0:
            cycle_type = "monthly"
        elif 350.0 <= period_days <= 380.0:
            cycle_type = "annual"
        else:
            cycle_type = f"~{period_days:.0f}d"

        topic_label = label_map.get(topic_id, topic_id)

        rows.append({
            "analysis_type": "cyclicality",
            "topic_id": str(topic_id),
            "topic_label": str(topic_label),
            "dominant_period_days": round(period_days, 1),
            "power_ratio": round(power_ratio, 2),
            "cycle_type": cycle_type,
        })

    if not rows:
        logger.warning("TP-4: no significant cyclical patterns detected")
        return pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "dominant_period_days", "power_ratio", "cycle_type",
        ])

    result_df = pd.DataFrame(rows)

    cycle_counts = result_df["cycle_type"].value_counts().to_dict()
    logger.info(
        "TP-4: detected %d cyclical topics — %s",
        len(result_df),
        ", ".join(f"{k}: {v}" for k, v in sorted(cycle_counts.items())),
    )

    return result_df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_temporal_analysis(
    corpus: WindowCorpus,
    output_dir: Path,
    prior_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Execute all 4 temporal metrics (TP-1 through TP-4).

    Loads articles and topics from the WindowCorpus, then computes each
    metric independently. Results are saved as Parquet files and returned
    as a dict for downstream P1 validation.

    Args:
        corpus: WindowCorpus providing lazy-loaded Parquet access.
        output_dir: Directory to write output Parquet files.
        prior_metrics: Metrics from previously completed modules (unused by M4).

    Returns:
        Dict with keys:
            velocity_map: dict[str, float] — mean lag hours per lang pair
            decay_types: dict[str, str] — topic_id -> decay type string
            cascade_pairs: dict[str, float] — "cause->effect" -> granger p-value
    """
    logger.info("=== M4 Temporal Pattern Analysis: start ===")

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    articles_df = corpus.load_parquet(
        "articles", columns=["article_id", "language", "published_at"]
    )
    topics_df = corpus.load_parquet(
        "topics", columns=["article_id", "topic_id", "topic_label"]
    )

    # Edge case: completely empty data
    if articles_df.empty or topics_df.empty:
        logger.warning(
            "M4: insufficient data (articles=%d, topics=%d), returning empty",
            len(articles_df), len(topics_df),
        )
        _write_empty_outputs(output_dir)
        return _empty_metrics()

    # ------------------------------------------------------------------
    # Merge _crawl_date from articles into topics if not present
    # ------------------------------------------------------------------
    merge_cols = ["article_id"]
    if "_crawl_date" in articles_df.columns and "_crawl_date" not in topics_df.columns:
        merge_cols.append("_crawl_date")
    if "language" not in topics_df.columns:
        merge_cols.append("language")

    if len(merge_cols) > 1:
        topics_df = topics_df.merge(
            articles_df[merge_cols].drop_duplicates("article_id"),
            on="article_id",
            how="inner",
        )

    available_dates = sorted(corpus.available_dates)

    # ------------------------------------------------------------------
    # TP-1: Event Cascade Detection (Granger Causality)
    # ------------------------------------------------------------------
    logger.info("--- TP-1: Event Cascade Detection ---")
    cascade_df = _compute_cascades(topics_df, available_dates, corpus.window_days)

    # ------------------------------------------------------------------
    # TP-2: Information Velocity Matrix
    # ------------------------------------------------------------------
    logger.info("--- TP-2: Information Velocity Matrix ---")
    # Check for multiple languages
    if "language" in topics_df.columns:
        n_langs = topics_df["language"].nunique()
    else:
        n_langs = articles_df["language"].nunique() if "language" in articles_df.columns else 0

    if n_langs < 2:
        logger.warning("TP-2: single language corpus, skipping velocity computation")
        velocity_df = pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "lang_pair", "first_lang", "second_lang", "lag_hours",
        ])
    else:
        velocity_df = _compute_velocity(topics_df, articles_df)

    # ------------------------------------------------------------------
    # Combine cascade + velocity into velocity_map.parquet
    # ------------------------------------------------------------------
    velocity_map_frames = []
    if not cascade_df.empty:
        velocity_map_frames.append(cascade_df)
    if not velocity_df.empty:
        velocity_map_frames.append(velocity_df)

    if velocity_map_frames:
        velocity_map_df = pd.concat(velocity_map_frames, ignore_index=True)
    else:
        velocity_map_df = pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "lang_pair", "first_lang", "second_lang", "lag_hours",
            "cause_topic", "effect_topic", "lag", "p_value",
        ])

    _write_parquet(velocity_map_df, output_dir / "velocity_map.parquet")

    # ------------------------------------------------------------------
    # TP-3: Attention Decay Classification
    # ------------------------------------------------------------------
    logger.info("--- TP-3: Attention Decay Classification ---")
    decay_df = _compute_decay_curves(topics_df, available_dates)

    # ------------------------------------------------------------------
    # TP-4: Structural Cyclicality Detection
    # ------------------------------------------------------------------
    logger.info("--- TP-4: Structural Cyclicality Detection ---")
    cyclicality_df = _compute_cyclicality(topics_df, available_dates)

    # ------------------------------------------------------------------
    # Combine decay + cyclicality into decay_curves.parquet
    # ------------------------------------------------------------------
    decay_frames = []
    if not decay_df.empty:
        decay_frames.append(decay_df)
    if not cyclicality_df.empty:
        decay_frames.append(cyclicality_df)

    if decay_frames:
        decay_curves_df = pd.concat(decay_frames, ignore_index=True)
    else:
        decay_curves_df = pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "peak_date", "peak_count", "post_peak_points",
            "r2_exponential", "r2_power_law", "lambda_exp",
            "alpha_pow", "decay_type",
            "dominant_period_days", "power_ratio", "cycle_type",
        ])

    _write_parquet(decay_curves_df, output_dir / "decay_curves.parquet")

    # ------------------------------------------------------------------
    # Build metrics dict for P1 validation
    # ------------------------------------------------------------------

    # velocity_map: mean lag hours per language pair (from TP-2)
    velocity_map: dict[str, float] = {}
    if not velocity_df.empty:
        mean_lags = velocity_df.groupby("lang_pair")["lag_hours"].mean()
        velocity_map = {k: round(v, 2) for k, v in mean_lags.to_dict().items()}

    # decay_types: topic_id -> decay type string (from TP-3)
    decay_types: dict[str, str] = {}
    if not decay_df.empty:
        decay_types = dict(zip(decay_df["topic_id"], decay_df["decay_type"]))

    # cascade_pairs: "cause->effect" -> granger p-value (from TP-1)
    cascade_pairs: dict[str, float] = {}
    if not cascade_df.empty:
        for _, row in cascade_df.iterrows():
            key = f"{row['cause_topic']}->{row['effect_topic']}"
            cascade_pairs[key] = float(row["p_value"])

    metrics: dict[str, Any] = {
        "velocity_map": velocity_map,
        "decay_types": decay_types,
        "cascade_pairs": cascade_pairs,
    }

    logger.info(
        "=== M4 Temporal Pattern Analysis: complete — "
        "velocity pairs=%d, decay topics=%d, cascade pairs=%d, "
        "cyclical topics=%d ===",
        len(velocity_map),
        len(decay_types),
        len(cascade_pairs),
        len(cyclicality_df),
    )

    return metrics


# ---------------------------------------------------------------------------
# Edge-case helpers
# ---------------------------------------------------------------------------

def _empty_metrics() -> dict[str, Any]:
    """Return the metrics dict shape with empty values."""
    return {
        "velocity_map": {},
        "decay_types": {},
        "cascade_pairs": {},
    }


def _write_empty_outputs(output_dir: Path) -> None:
    """Write minimal valid Parquet files when data is insufficient.

    Ensures L0 output validation passes (files exist with valid schema)
    even when there is not enough data to compute meaningful metrics.
    """
    _write_parquet(
        pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "lang_pair", "first_lang", "second_lang", "lag_hours",
        ]),
        output_dir / "velocity_map.parquet",
    )
    _write_parquet(
        pd.DataFrame(columns=[
            "analysis_type", "topic_id", "topic_label",
            "peak_date", "peak_count", "decay_type",
        ]),
        output_dir / "decay_curves.parquet",
    )
    logger.info("M4: wrote empty placeholder Parquet files")
