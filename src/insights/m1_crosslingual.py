"""M1: Cross-Lingual Analysis — Information asymmetry, attention gaps, sentiment
divergence, and filter bubbles across language communities.

4 metrics (CL-1 through CL-4), all Type A (pure arithmetic).
NO LLM calls. NO ML model inference.

CL-1: Information Asymmetry Index (Jensen-Shannon Divergence)
CL-2: Attention Gap Matrix
CL-3: Sentiment Polarity Divergence (Wasserstein distance)
CL-4: Filter Bubble Index (Jaccard similarity)

Input:  Stage 1-4 Parquet files via WindowCorpus (READ-ONLY)
Output: 4 Parquet files + metrics dict for P1 validation

Reference: research/bigdata-insight-workflow-design.md, M1 specification.
"""

from __future__ import annotations

import logging
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.spatial.distance import jensenshannon
from scipy.stats import wasserstein_distance

from src.config.constants import (
    INSIGHT_MIN_ARTICLES_PER_LANG,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)
from src.insights.constants import (
    ATTENTION_GAP_EPSILON,
    JSD_BASELINE_WINDOW_DAYS,
    JSD_SPIKE_THRESHOLD,
    SENTIMENT_DIVERGENCE_THRESHOLD,
)
from src.insights.window_assembler import WindowCorpus

logger = logging.getLogger(__name__)

# Minimum articles per language per topic for sentiment divergence (CL-3)
_CL3_MIN_ARTICLES_PER_LANG_TOPIC = 10

# Minimum articles per language per topic for filter bubble inclusion (CL-4)
_CL4_MIN_ARTICLES_PER_TOPIC = 5


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


def _filter_languages(df: pd.DataFrame, min_articles: int) -> list[str]:
    """Return languages meeting the minimum article count threshold.

    Args:
        df: DataFrame with a ``language`` column.
        min_articles: Minimum article count per language.

    Returns:
        Sorted list of qualifying language codes.
    """
    counts = df["language"].value_counts()
    qualifying = counts[counts >= min_articles].index.tolist()
    return sorted(qualifying)


def _lang_pair_key(lang_a: str, lang_b: str) -> str:
    """Canonical key for a language pair (alphabetically sorted)."""
    a, b = sorted([lang_a, lang_b])
    return f"{a}-{b}"


# ---------------------------------------------------------------------------
# CL-1: Information Asymmetry Index (JSD)
# ---------------------------------------------------------------------------

def _compute_jsd(
    topics_df: pd.DataFrame,
    valid_langs: list[str],
    available_dates: list[str],
) -> pd.DataFrame:
    """Compute daily Jensen-Shannon Divergence between per-language topic distributions.

    For each date, we build P(topic | lang) = count(topic, lang) / total(lang).
    JSD is computed for every language pair using scipy's jensenshannon (which
    returns the distance, i.e. sqrt(JSD); we square it to get proper divergence).

    A rolling baseline (mean JSD over the last ``JSD_BASELINE_WINDOW_DAYS`` days)
    and delta (current - baseline) are appended.

    Args:
        topics_df: DataFrame with columns [article_id, topic_id, language, _crawl_date].
        valid_langs: Language codes meeting the article threshold.
        available_dates: Sorted list of date strings in the window.

    Returns:
        DataFrame with columns:
            date, lang_pair, jsd, rolling_baseline, delta, is_spike
    """
    all_topic_ids = topics_df["topic_id"].unique()
    topic_to_idx = {t: i for i, t in enumerate(sorted(all_topic_ids))}
    n_topics = len(topic_to_idx)

    if n_topics == 0:
        logger.warning("CL-1: no topics found, returning empty JSD result")
        return pd.DataFrame(
            columns=["date", "lang_pair", "jsd", "rolling_baseline", "delta", "is_spike"]
        )

    lang_pairs = list(combinations(valid_langs, 2))
    rows: list[dict[str, Any]] = []

    for d in sorted(available_dates):
        day_df = topics_df[topics_df["_crawl_date"] == d]
        if day_df.empty:
            continue

        # Build per-language topic distributions
        distributions: dict[str, np.ndarray] = {}
        for lang in valid_langs:
            lang_day = day_df[day_df["language"] == lang]
            if lang_day.empty:
                continue
            counts = np.zeros(n_topics, dtype=np.float64)
            for tid in lang_day["topic_id"]:
                if tid in topic_to_idx:
                    counts[topic_to_idx[tid]] += 1.0
            total = counts.sum()
            if total > 0:
                distributions[lang] = counts / total

        # Compute JSD for each language pair present this day
        for lang_a, lang_b in lang_pairs:
            if lang_a not in distributions or lang_b not in distributions:
                continue
            # scipy jensenshannon returns sqrt(JSD); square to get divergence in [0, 1]
            jsd_dist = jensenshannon(distributions[lang_a], distributions[lang_b])
            jsd_val = float(jsd_dist ** 2)
            rows.append({
                "date": d,
                "lang_pair": _lang_pair_key(lang_a, lang_b),
                "jsd": jsd_val,
            })

    if not rows:
        logger.warning("CL-1: no JSD values computed")
        return pd.DataFrame(
            columns=["date", "lang_pair", "jsd", "rolling_baseline", "delta", "is_spike"]
        )

    result_df = pd.DataFrame(rows)

    # Rolling baseline and delta per language pair
    baseline_values: list[float] = []
    delta_values: list[float] = []
    spike_flags: list[bool] = []

    for _, group in result_df.groupby("lang_pair"):
        group_sorted = group.sort_values("date")
        rolling = (
            group_sorted["jsd"]
            .rolling(window=JSD_BASELINE_WINDOW_DAYS, min_periods=1)
            .mean()
        )
        deltas = group_sorted["jsd"] - rolling
        baseline_values.extend(rolling.tolist())
        delta_values.extend(deltas.tolist())
        spike_flags.extend((deltas > JSD_SPIKE_THRESHOLD).tolist())

    # Rebuild with sorted order to align with groupby iteration
    result_df = result_df.sort_values(["lang_pair", "date"]).reset_index(drop=True)
    result_df["rolling_baseline"] = baseline_values
    result_df["delta"] = delta_values
    result_df["is_spike"] = spike_flags

    logger.info(
        "CL-1: computed %d JSD values across %d language pairs, %d spikes",
        len(result_df),
        len(lang_pairs),
        result_df["is_spike"].sum(),
    )
    return result_df


# ---------------------------------------------------------------------------
# CL-2: Attention Gap Matrix
# ---------------------------------------------------------------------------

def _compute_attention_gaps(
    topics_df: pd.DataFrame,
    valid_langs: list[str],
) -> pd.DataFrame:
    """Compute the attention gap for each topic across language communities.

    Attention fraction: attention(topic, lang) = count(topic in lang) / total(lang).
    Gap = max_lang(attention) - min_lang(attention).
    A topic is flagged as having a "blind spot" if attention < ATTENTION_GAP_EPSILON
    in any language.

    Args:
        topics_df: DataFrame with columns [topic_id, topic_label, language].
        valid_langs: Language codes meeting the article threshold.

    Returns:
        DataFrame with columns:
            topic_id, topic_label, gap, max_lang, min_lang,
            max_attention, min_attention, blind_spot_langs,
            + one column per language with the attention fraction
    """
    if topics_df.empty:
        logger.warning("CL-2: empty topics data, returning empty attention gaps")
        return pd.DataFrame()

    # Total articles per language
    lang_totals = topics_df.groupby("language").size()

    # Count per (topic, language)
    topic_lang_counts = (
        topics_df.groupby(["topic_id", "topic_label", "language"])
        .size()
        .reset_index(name="count")
    )

    rows: list[dict[str, Any]] = []

    for (topic_id, topic_label), group in topic_lang_counts.groupby(
        ["topic_id", "topic_label"]
    ):
        per_lang: dict[str, float] = {}
        for lang in valid_langs:
            lang_row = group[group["language"] == lang]
            count = int(lang_row["count"].iloc[0]) if not lang_row.empty else 0
            total = int(lang_totals.get(lang, 1))
            per_lang[lang] = count / total if total > 0 else 0.0

        if not per_lang:
            continue

        attentions = list(per_lang.values())
        max_att = max(attentions)
        min_att = min(attentions)
        gap = max_att - min_att

        max_lang = max(per_lang, key=per_lang.get)  # type: ignore[arg-type]
        min_lang = min(per_lang, key=per_lang.get)  # type: ignore[arg-type]

        blind_spots = [
            lang for lang, att in per_lang.items()
            if att < ATTENTION_GAP_EPSILON
        ]

        row: dict[str, Any] = {
            "topic_id": topic_id,
            "topic_label": topic_label,
            "gap": gap,
            "max_lang": max_lang,
            "min_lang": min_lang,
            "max_attention": max_att,
            "min_attention": min_att,
            "blind_spot_langs": ",".join(blind_spots) if blind_spots else "",
        }
        # Per-language attention columns
        for lang in valid_langs:
            row[f"attention_{lang}"] = per_lang.get(lang, 0.0)

        rows.append(row)

    if not rows:
        logger.warning("CL-2: no attention gaps computed")
        return pd.DataFrame()

    result_df = pd.DataFrame(rows).sort_values("gap", ascending=False).reset_index(drop=True)

    logger.info(
        "CL-2: computed attention gaps for %d topics, %d with blind spots",
        len(result_df),
        (result_df["blind_spot_langs"] != "").sum(),
    )
    return result_df


# ---------------------------------------------------------------------------
# CL-3: Sentiment Polarity Divergence
# ---------------------------------------------------------------------------

def _compute_sentiment_divergence(
    topics_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    valid_langs: list[str],
) -> pd.DataFrame:
    """Compute Wasserstein distance of sentiment distributions per aligned topic.

    For each topic appearing in 2+ languages (with >= _CL3_MIN_ARTICLES_PER_LANG_TOPIC
    articles per language), compute pairwise Wasserstein distance of sentiment_score
    distributions.

    Args:
        topics_df: DataFrame with [article_id, topic_id, topic_label, language].
        analysis_df: DataFrame with [article_id, sentiment_score].
        valid_langs: Language codes meeting the article threshold.

    Returns:
        DataFrame with columns:
            topic_id, topic_label, lang_pair, wasserstein_dist, is_significant
    """
    # Merge topics with sentiment scores
    merged = topics_df.merge(
        analysis_df[["article_id", "sentiment_score"]],
        on="article_id",
        how="inner",
    )

    if merged.empty:
        logger.warning("CL-3: no merged topic-sentiment data")
        return pd.DataFrame(
            columns=["topic_id", "topic_label", "lang_pair",
                      "wasserstein_dist", "is_significant"]
        )

    # Drop rows with missing sentiment
    merged = merged.dropna(subset=["sentiment_score"])

    rows: list[dict[str, Any]] = []

    for (topic_id, topic_label), topic_group in merged.groupby(
        ["topic_id", "topic_label"]
    ):
        # Collect per-language sentiment distributions
        lang_sentiments: dict[str, np.ndarray] = {}
        for lang in valid_langs:
            lang_data = topic_group[topic_group["language"] == lang]["sentiment_score"]
            if len(lang_data) >= _CL3_MIN_ARTICLES_PER_LANG_TOPIC:
                lang_sentiments[lang] = lang_data.values.astype(np.float64)

        if len(lang_sentiments) < 2:
            continue

        # Pairwise Wasserstein distance
        for lang_a, lang_b in combinations(sorted(lang_sentiments.keys()), 2):
            w_dist = float(wasserstein_distance(
                lang_sentiments[lang_a],
                lang_sentiments[lang_b],
            ))
            rows.append({
                "topic_id": topic_id,
                "topic_label": topic_label,
                "lang_pair": _lang_pair_key(lang_a, lang_b),
                "wasserstein_dist": w_dist,
                "is_significant": w_dist > SENTIMENT_DIVERGENCE_THRESHOLD,
            })

    if not rows:
        logger.warning("CL-3: no sentiment divergence values computed")
        return pd.DataFrame(
            columns=["topic_id", "topic_label", "lang_pair",
                      "wasserstein_dist", "is_significant"]
        )

    result_df = pd.DataFrame(rows).sort_values(
        "wasserstein_dist", ascending=False
    ).reset_index(drop=True)

    n_significant = result_df["is_significant"].sum()
    logger.info(
        "CL-3: computed %d Wasserstein distances, %d significant (> %.2f)",
        len(result_df), n_significant, SENTIMENT_DIVERGENCE_THRESHOLD,
    )
    return result_df


# ---------------------------------------------------------------------------
# CL-4: Filter Bubble Index
# ---------------------------------------------------------------------------

def _compute_filter_bubble(
    topics_df: pd.DataFrame,
    valid_langs: list[str],
) -> pd.DataFrame:
    """Compute Jaccard similarity of topic sets between language communities.

    For each language, the topic set contains topic_ids with
    >= _CL4_MIN_ARTICLES_PER_TOPIC articles. Jaccard similarity measures
    the overlap of these topic sets between all language pairs.

    Low Jaccard = strong filter bubble (communities see different topics).
    High Jaccard = weak filter bubble (communities see similar topics).

    Args:
        topics_df: DataFrame with [article_id, topic_id, language].
        valid_langs: Language codes meeting the article threshold.

    Returns:
        DataFrame with columns:
            lang_pair, jaccard, intersection_size, union_size
    """
    # Build topic sets per language
    lang_topic_sets: dict[str, set] = {}
    for lang in valid_langs:
        lang_df = topics_df[topics_df["language"] == lang]
        topic_counts = lang_df.groupby("topic_id").size()
        qualifying = set(topic_counts[topic_counts >= _CL4_MIN_ARTICLES_PER_TOPIC].index)
        lang_topic_sets[lang] = qualifying

    rows: list[dict[str, Any]] = []
    for lang_a, lang_b in combinations(valid_langs, 2):
        set_a = lang_topic_sets.get(lang_a, set())
        set_b = lang_topic_sets.get(lang_b, set())
        union = set_a | set_b
        intersection = set_a & set_b

        if len(union) == 0:
            jaccard = 0.0
        else:
            jaccard = len(intersection) / len(union)

        rows.append({
            "lang_pair": _lang_pair_key(lang_a, lang_b),
            "jaccard": jaccard,
            "intersection_size": len(intersection),
            "union_size": len(union),
        })

    if not rows:
        logger.warning("CL-4: no filter bubble values computed")
        return pd.DataFrame(
            columns=["lang_pair", "jaccard", "intersection_size", "union_size"]
        )

    result_df = pd.DataFrame(rows).sort_values("jaccard").reset_index(drop=True)

    logger.info(
        "CL-4: computed %d Jaccard similarities, mean=%.3f, min=%.3f, max=%.3f",
        len(result_df),
        result_df["jaccard"].mean(),
        result_df["jaccard"].min(),
        result_df["jaccard"].max(),
    )
    return result_df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_crosslingual_analysis(
    corpus: WindowCorpus,
    output_dir: Path,
    prior_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Execute all 4 cross-lingual metrics (CL-1 through CL-4).

    Loads articles, article_analysis, and topics from the WindowCorpus,
    filters languages below the minimum article threshold, then computes
    each metric independently. Results are saved as Parquet files and
    returned as a dict for downstream P1 validation.

    Args:
        corpus: WindowCorpus providing lazy-loaded Parquet access.
        output_dir: Directory to write output Parquet files.
        prior_metrics: Metrics from previously completed modules (unused by M1).

    Returns:
        Dict with keys:
            jsd_values: dict[str, float] — latest-day JSD per language pair
            attention_gaps: list[dict] — per-topic attention gap summaries
            sentiment_divergence: dict[str, float] — mean Wasserstein per lang pair
            filter_bubble: dict[str, float] — Jaccard per language pair
    """
    logger.info("=== M1 Cross-Lingual Analysis: start ===")

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    articles_df = corpus.load_parquet(
        "articles", columns=["article_id", "language"]
    )
    topics_df = corpus.load_parquet(
        "topics", columns=["article_id", "topic_id", "topic_label"]
    )
    analysis_df = corpus.load_parquet(
        "article_analysis", columns=["article_id", "sentiment_score"]
    )

    # Edge case: completely empty data
    if articles_df.empty or topics_df.empty:
        logger.warning("M1: insufficient data (articles=%d, topics=%d), returning empty",
                        len(articles_df), len(topics_df))
        _write_empty_outputs(output_dir)
        return _empty_metrics()

    # ------------------------------------------------------------------
    # Merge language into topics (topics.parquet may not have language)
    # ------------------------------------------------------------------
    # Merge language + _crawl_date from articles into topics
    merge_cols = ["article_id", "language"]
    if "_crawl_date" in articles_df.columns and "_crawl_date" not in topics_df.columns:
        merge_cols.append("_crawl_date")
    topics_df = topics_df.merge(
        articles_df[merge_cols].drop_duplicates("article_id"),
        on="article_id",
        how="inner",
    )

    # ------------------------------------------------------------------
    # Filter languages below threshold
    # ------------------------------------------------------------------
    valid_langs = _filter_languages(articles_df, INSIGHT_MIN_ARTICLES_PER_LANG)
    logger.info(
        "M1: %d languages meet threshold (%d articles): %s",
        len(valid_langs), INSIGHT_MIN_ARTICLES_PER_LANG, valid_langs,
    )

    if len(valid_langs) < 2:
        logger.warning(
            "M1: need >= 2 qualifying languages, got %d. Returning empty.",
            len(valid_langs),
        )
        _write_empty_outputs(output_dir)
        return _empty_metrics()

    # Restrict topics to valid languages
    topics_df = topics_df[topics_df["language"].isin(valid_langs)].copy()

    # ------------------------------------------------------------------
    # CL-1: Information Asymmetry Index (JSD)
    # ------------------------------------------------------------------
    logger.info("--- CL-1: Information Asymmetry Index (JSD) ---")
    available_dates = sorted(corpus.available_dates)
    jsd_df = _compute_jsd(topics_df, valid_langs, available_dates)
    _write_parquet(jsd_df, output_dir / "asymmetry_index.parquet")

    # Extract latest-day JSD for validation dict
    jsd_values: dict[str, float] = {}
    if not jsd_df.empty:
        latest_date = jsd_df["date"].max()
        latest_jsd = jsd_df[jsd_df["date"] == latest_date]
        jsd_values = dict(zip(latest_jsd["lang_pair"], latest_jsd["jsd"]))

    # ------------------------------------------------------------------
    # CL-2: Attention Gap Matrix
    # ------------------------------------------------------------------
    logger.info("--- CL-2: Attention Gap Matrix ---")
    attention_df = _compute_attention_gaps(topics_df, valid_langs)
    if not attention_df.empty:
        _write_parquet(attention_df, output_dir / "attention_gaps.parquet")
    else:
        _write_parquet(
            pd.DataFrame(columns=["topic_id", "topic_label", "gap"]),
            output_dir / "attention_gaps.parquet",
        )

    # Build validation-friendly summary
    attention_gaps_summary: list[dict[str, Any]] = []
    if not attention_df.empty:
        for _, row in attention_df.iterrows():
            per_lang = {
                col.replace("attention_", ""): float(row[col])
                for col in attention_df.columns
                if col.startswith("attention_")
            }
            attention_gaps_summary.append({
                "topic": row["topic_id"],
                "gap": float(row["gap"]),
                "per_lang": per_lang,
            })

    # ------------------------------------------------------------------
    # CL-3: Sentiment Polarity Divergence
    # ------------------------------------------------------------------
    logger.info("--- CL-3: Sentiment Polarity Divergence ---")
    sentiment_df = _compute_sentiment_divergence(topics_df, analysis_df, valid_langs)
    _write_parquet(sentiment_df, output_dir / "sentiment_divergence.parquet")

    # Mean Wasserstein distance per language pair for validation
    sentiment_divergence: dict[str, float] = {}
    if not sentiment_df.empty:
        mean_per_pair = sentiment_df.groupby("lang_pair")["wasserstein_dist"].mean()
        sentiment_divergence = mean_per_pair.to_dict()

    # ------------------------------------------------------------------
    # CL-4: Filter Bubble Index
    # ------------------------------------------------------------------
    logger.info("--- CL-4: Filter Bubble Index ---")
    bubble_df = _compute_filter_bubble(topics_df, valid_langs)
    _write_parquet(bubble_df, output_dir / "filter_bubble.parquet")

    # Jaccard per language pair for validation
    filter_bubble: dict[str, float] = {}
    if not bubble_df.empty:
        filter_bubble = dict(zip(bubble_df["lang_pair"], bubble_df["jaccard"]))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    metrics: dict[str, Any] = {
        "jsd_values": jsd_values,
        "attention_gaps": attention_gaps_summary,
        "sentiment_divergence": sentiment_divergence,
        "filter_bubble": filter_bubble,
    }

    logger.info(
        "=== M1 Cross-Lingual Analysis: complete — "
        "JSD pairs=%d, attention topics=%d, sentiment pairs=%d, bubble pairs=%d ===",
        len(jsd_values),
        len(attention_gaps_summary),
        len(sentiment_divergence),
        len(filter_bubble),
    )

    return metrics


# ---------------------------------------------------------------------------
# Edge-case helpers
# ---------------------------------------------------------------------------

def _empty_metrics() -> dict[str, Any]:
    """Return the metrics dict shape with empty values."""
    return {
        "jsd_values": {},
        "attention_gaps": [],
        "sentiment_divergence": {},
        "filter_bubble": {},
    }


def _write_empty_outputs(output_dir: Path) -> None:
    """Write minimal valid Parquet files when data is insufficient.

    Ensures L0 output validation passes (files exist with valid schema)
    even when there is not enough data to compute meaningful metrics.
    """
    _write_parquet(
        pd.DataFrame(columns=["date", "lang_pair", "jsd",
                               "rolling_baseline", "delta", "is_spike"]),
        output_dir / "asymmetry_index.parquet",
    )
    _write_parquet(
        pd.DataFrame(columns=["topic_id", "topic_label", "gap"]),
        output_dir / "attention_gaps.parquet",
    )
    _write_parquet(
        pd.DataFrame(columns=["topic_id", "topic_label", "lang_pair",
                               "wasserstein_dist", "is_significant"]),
        output_dir / "sentiment_divergence.parquet",
    )
    _write_parquet(
        pd.DataFrame(columns=["lang_pair", "jaccard",
                               "intersection_size", "union_size"]),
        output_dir / "filter_bubble.parquet",
    )
    logger.info("M1: wrote empty placeholder Parquet files")
