"""M3: Entity Analytics — Trajectory classification, hidden connections,
emergence index, and cross-language entity reach.

4 metrics (EA-1 through EA-4), all Type A (pure arithmetic) or Type B (rule-based).
NO LLM calls. NO ML model inference.

EA-1: Entity Trajectory Classification
      Weekly PageRank time series classified as Rising Star / Fading Giant /
      Cyclical / Burst / Plateau using deterministic slope + CV rules.

EA-2: Hidden Connection Discovery
      Entities that never co-occur directly but have high Jaccard similarity
      on their neighbor sets in the co-occurrence graph.

EA-3: Entity Emergence Index
      First-appearance date + 14-day mention acceleration (2nd derivative
      of cumulative mentions).

EA-4: Cross-Language Entity Reach
      Number of distinct languages an entity appears in, tracked over time.

Input:  Stage 1-4 Parquet files via WindowCorpus (READ-ONLY)
Output: 2 Parquet files + metrics dict for P1 validation

Reference: research/bigdata-insight-workflow-design.md, M3 specification.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.constants import (
    INSIGHT_MIN_ENTITY_MENTIONS,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)
from src.insights.constants import (
    EMERGENCE_ACCELERATION_MIN,
    HIDDEN_CONNECTION_JACCARD_MIN,
    TRAJECTORY_BURST_CV,
    TRAJECTORY_FADING_SLOPE,
    TRAJECTORY_PLATEAU_SLOPE,
    TRAJECTORY_RISING_SLOPE,
)
from src.insights.window_assembler import WindowCorpus

logger = logging.getLogger(__name__)

# Maximum entities for O(N^2) hidden connection search (EA-2)
_EA2_MAX_ENTITIES = 200

# Days after first appearance for emergence acceleration (EA-3)
_EA3_EMERGENCE_WINDOW_DAYS = 14

# Minimum weeks of PageRank data for trajectory classification (EA-1)
_EA1_MIN_WEEKS = 2


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


def _explode_entities(ner_df: pd.DataFrame) -> pd.DataFrame:
    """Explode NER entity lists into one row per (article_id, entity, entity_type).

    NER columns (entities_person, entities_org, entities_location) contain
    lists of strings per article. This function normalizes them into a long
    format suitable for counting and graph construction.

    Args:
        ner_df: DataFrame with article_id and NER list columns.

    Returns:
        DataFrame with columns: article_id, entity, entity_type
    """
    entity_cols = {
        "entities_person": "person",
        "entities_org": "org",
        "entities_location": "location",
    }

    frames: list[pd.DataFrame] = []

    for col, etype in entity_cols.items():
        if col not in ner_df.columns:
            logger.debug("NER column %s not found, skipping", col)
            continue

        subset = ner_df[["article_id", col]].copy()
        subset = subset.rename(columns={col: "entities"})

        # Handle non-list entries (strings, NaN, numpy arrays)
        def _ensure_list(val: Any) -> list:
            if isinstance(val, np.ndarray):
                return val.tolist()
            if isinstance(val, list):
                return val
            if isinstance(val, str):
                # Try to parse stringified list
                val_stripped = val.strip()
                if val_stripped.startswith("["):
                    try:
                        import ast
                        parsed = ast.literal_eval(val_stripped)
                        if isinstance(parsed, list):
                            return parsed
                    except (ValueError, SyntaxError):
                        pass
                # Single entity as string
                if val_stripped:
                    return [val_stripped]
            return []

        subset["entities"] = subset["entities"].apply(_ensure_list)
        subset = subset.explode("entities", ignore_index=True)
        subset = subset.dropna(subset=["entities"])
        subset = subset[subset["entities"].astype(str).str.strip() != ""]
        subset["entity"] = subset["entities"].astype(str).str.strip()
        subset["entity_type"] = etype
        frames.append(subset[["article_id", "entity", "entity_type"]])

    if not frames:
        return pd.DataFrame(columns=["article_id", "entity", "entity_type"])

    result = pd.concat(frames, ignore_index=True)
    logger.info(
        "Exploded NER: %d entity mentions from %d articles",
        len(result), result["article_id"].nunique(),
    )
    return result


def _assign_week(date_str: str) -> str:
    """Convert a date string (YYYY-MM-DD) to ISO week key (YYYY-WNN).

    Args:
        date_str: Date string in YYYY-MM-DD format.

    Returns:
        Week key string like '2026-W14'.
    """
    try:
        dt = pd.Timestamp(date_str)
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# EA-1: Entity Trajectory Classification
# ---------------------------------------------------------------------------

def classify_trajectory(pagerank_series: list[float]) -> str:
    """Classify an entity's PageRank trajectory using slope and coefficient of variation.

    Rules (evaluated in order):
        1. slope > TRAJECTORY_RISING_SLOPE and cv < 0.5 -> rising_star
        2. slope < TRAJECTORY_FADING_SLOPE and cv < 0.5 -> fading_giant
        3. cv > TRAJECTORY_BURST_CV -> burst
        4. abs(slope) < TRAJECTORY_PLATEAU_SLOPE -> plateau
        5. else -> cyclical

    Args:
        pagerank_series: List of weekly PageRank values (chronological order).

    Returns:
        One of: 'rising_star', 'fading_giant', 'burst', 'plateau', 'cyclical'.
    """
    if len(pagerank_series) < 2:
        return "plateau"

    series = np.array(pagerank_series, dtype=np.float64)
    slope = float(np.polyfit(range(len(series)), series, 1)[0])
    mean_val = float(np.mean(series))
    cv = float(np.std(series) / (mean_val + 1e-10))

    if slope > TRAJECTORY_RISING_SLOPE and cv < 0.5:
        return "rising_star"
    elif slope < TRAJECTORY_FADING_SLOPE and cv < 0.5:
        return "fading_giant"
    elif cv > TRAJECTORY_BURST_CV:
        return "burst"
    elif abs(slope) < TRAJECTORY_PLATEAU_SLOPE:
        return "plateau"
    else:
        return "cyclical"


def _compute_trajectories(
    entity_mentions: pd.DataFrame,
    min_mentions: int,
) -> pd.DataFrame:
    """Compute entity trajectories from weekly PageRank time series.

    For each week in the data, builds a co-occurrence graph (entities
    appearing in the same article share an edge), computes PageRank,
    then classifies each entity's PageRank trajectory across weeks.

    Args:
        entity_mentions: DataFrame with columns [article_id, entity, _crawl_date].
        min_mentions: Minimum total mentions for an entity to be included.

    Returns:
        DataFrame with columns:
            entity, trajectory_type, total_mentions, num_weeks,
            slope, cv, pagerank_series (list of floats per week)
    """
    if entity_mentions.empty:
        logger.warning("EA-1: no entity mentions, returning empty trajectories")
        return pd.DataFrame(
            columns=["entity", "trajectory_type", "total_mentions",
                     "num_weeks", "slope", "cv", "pagerank_series"]
        )

    # Filter entities by minimum mention count
    mention_counts = entity_mentions["entity"].value_counts()
    qualifying_entities = set(mention_counts[mention_counts >= min_mentions].index)

    if not qualifying_entities:
        logger.warning(
            "EA-1: no entities with >= %d mentions (max=%d)",
            min_mentions, mention_counts.max() if len(mention_counts) > 0 else 0,
        )
        return pd.DataFrame(
            columns=["entity", "trajectory_type", "total_mentions",
                     "num_weeks", "slope", "cv", "pagerank_series"]
        )

    # Filter to qualifying entities
    filtered = entity_mentions[entity_mentions["entity"].isin(qualifying_entities)].copy()
    filtered["week"] = filtered["_crawl_date"].apply(_assign_week)

    # Build weekly co-occurrence graphs and compute PageRank
    weeks_sorted = sorted(filtered["week"].unique())
    weekly_pagerank: dict[str, dict[str, float]] = {}  # week -> {entity -> PR}

    for week in weeks_sorted:
        week_data = filtered[filtered["week"] == week]

        # Build co-occurrence graph: entities in the same article share an edge
        G = nx.Graph()
        for _, group in week_data.groupby("article_id"):
            entities_in_article = group["entity"].unique().tolist()
            for e in entities_in_article:
                if not G.has_node(e):
                    G.add_node(e)
            for e1, e2 in combinations(entities_in_article, 2):
                if G.has_edge(e1, e2):
                    G[e1][e2]["weight"] += 1
                else:
                    G.add_edge(e1, e2, weight=1)

        if G.number_of_nodes() == 0:
            continue

        # Compute PageRank
        try:
            pr = nx.pagerank(G, weight="weight")
        except nx.PowerIterationFailedConvergence:
            # Fallback: uniform
            n = G.number_of_nodes()
            pr = {node: 1.0 / n for node in G.nodes()}

        weekly_pagerank[week] = pr

    if not weekly_pagerank:
        logger.warning("EA-1: no weekly graphs produced")
        return pd.DataFrame(
            columns=["entity", "trajectory_type", "total_mentions",
                     "num_weeks", "slope", "cv", "pagerank_series"]
        )

    # For each qualifying entity, collect weekly PageRank series
    weeks_in_order = sorted(weekly_pagerank.keys())
    rows: list[dict[str, Any]] = []

    for entity in qualifying_entities:
        pr_series = [
            weekly_pagerank[w].get(entity, 0.0)
            for w in weeks_in_order
        ]

        # Only classify if enough weeks with non-zero values
        nonzero_weeks = sum(1 for v in pr_series if v > 0)
        if nonzero_weeks < _EA1_MIN_WEEKS:
            trajectory = "plateau"
        else:
            trajectory = classify_trajectory(pr_series)

        total = int(mention_counts.get(entity, 0))

        # Compute slope and CV for output
        series_arr = np.array(pr_series, dtype=np.float64)
        if len(series_arr) >= 2:
            slope = float(np.polyfit(range(len(series_arr)), series_arr, 1)[0])
            mean_val = float(np.mean(series_arr))
            cv = float(np.std(series_arr) / (mean_val + 1e-10))
        else:
            slope = 0.0
            cv = 0.0

        rows.append({
            "entity": entity,
            "trajectory_type": trajectory,
            "total_mentions": total,
            "num_weeks": len(weeks_in_order),
            "slope": round(slope, 8),
            "cv": round(cv, 6),
            "pagerank_series": pr_series,
        })

    result_df = pd.DataFrame(rows).sort_values(
        "total_mentions", ascending=False
    ).reset_index(drop=True)

    # Log distribution
    type_counts = result_df["trajectory_type"].value_counts().to_dict()
    logger.info(
        "EA-1: classified %d entities — %s",
        len(result_df), type_counts,
    )
    return result_df


# ---------------------------------------------------------------------------
# EA-2: Hidden Connection Discovery
# ---------------------------------------------------------------------------

def _compute_hidden_connections(
    entity_mentions: pd.DataFrame,
    min_mentions: int,
    max_entities: int = _EA2_MAX_ENTITIES,
) -> pd.DataFrame:
    """Discover hidden connections via Jaccard similarity of neighbor sets.

    Two entities are a "hidden connection" if they:
    1. Never directly co-occur in the same article, AND
    2. Have Jaccard(neighbors(A), neighbors(B)) >= HIDDEN_CONNECTION_JACCARD_MIN

    To keep computation tractable (O(N^2)), only the top ``max_entities``
    entities by mention count are considered.

    Args:
        entity_mentions: DataFrame with columns [article_id, entity].
        min_mentions: Minimum mentions to include an entity.
        max_entities: Maximum entities for the O(N^2) comparison.

    Returns:
        DataFrame with columns:
            entity_a, entity_b, jaccard, shared_neighbors, neighbors_a, neighbors_b
    """
    if entity_mentions.empty:
        logger.warning("EA-2: no entity mentions, returning empty hidden connections")
        return pd.DataFrame(
            columns=["entity_a", "entity_b", "jaccard",
                     "shared_neighbors", "neighbors_a", "neighbors_b"]
        )

    # Select top entities by mention count
    mention_counts = entity_mentions["entity"].value_counts()
    qualifying = mention_counts[mention_counts >= min_mentions].head(max_entities)
    top_entities = set(qualifying.index)

    if len(top_entities) < 2:
        logger.warning("EA-2: fewer than 2 qualifying entities")
        return pd.DataFrame(
            columns=["entity_a", "entity_b", "jaccard",
                     "shared_neighbors", "neighbors_a", "neighbors_b"]
        )

    # Build co-occurrence graph: entities in the same article
    filtered = entity_mentions[entity_mentions["entity"].isin(top_entities)]
    G = nx.Graph()
    for entity in top_entities:
        G.add_node(entity)

    for _, group in filtered.groupby("article_id"):
        entities_in_article = [
            e for e in group["entity"].unique() if e in top_entities
        ]
        for e1, e2 in combinations(entities_in_article, 2):
            if G.has_edge(e1, e2):
                G[e1][e2]["weight"] += 1
            else:
                G.add_edge(e1, e2, weight=1)

    # For each pair of entities that do NOT share an edge, compute Jaccard
    entities_list = sorted(top_entities)
    rows: list[dict[str, Any]] = []

    for i, e1 in enumerate(entities_list):
        neighbors_1 = set(G.neighbors(e1))
        for e2 in entities_list[i + 1:]:
            # Skip if they directly co-occur
            if G.has_edge(e1, e2):
                continue

            neighbors_2 = set(G.neighbors(e2))
            union = neighbors_1 | neighbors_2

            if len(union) == 0:
                continue

            intersection = neighbors_1 & neighbors_2
            jaccard = len(intersection) / len(union)

            if jaccard >= HIDDEN_CONNECTION_JACCARD_MIN:
                rows.append({
                    "entity_a": e1,
                    "entity_b": e2,
                    "jaccard": round(jaccard, 6),
                    "shared_neighbors": len(intersection),
                    "neighbors_a": len(neighbors_1),
                    "neighbors_b": len(neighbors_2),
                })

    if not rows:
        logger.info(
            "EA-2: no hidden connections found (Jaccard >= %.2f) among %d entities",
            HIDDEN_CONNECTION_JACCARD_MIN, len(top_entities),
        )
        return pd.DataFrame(
            columns=["entity_a", "entity_b", "jaccard",
                     "shared_neighbors", "neighbors_a", "neighbors_b"]
        )

    result_df = pd.DataFrame(rows).sort_values(
        "jaccard", ascending=False
    ).reset_index(drop=True)

    logger.info(
        "EA-2: found %d hidden connections among %d entities (Jaccard >= %.2f)",
        len(result_df), len(top_entities), HIDDEN_CONNECTION_JACCARD_MIN,
    )
    return result_df


# ---------------------------------------------------------------------------
# EA-3: Entity Emergence Index
# ---------------------------------------------------------------------------

def _compute_emergence_index(
    entity_mentions: pd.DataFrame,
) -> pd.DataFrame:
    """Compute entity emergence index based on first-appearance and mention acceleration.

    For each entity:
    1. Find first_appearance date
    2. Count daily mentions for 14 days after first appearance
    3. Compute cumulative mentions and fit linear regression -> slope = emergence speed
    4. Compute 2nd derivative (acceleration) to distinguish accelerating vs decelerating

    Args:
        entity_mentions: DataFrame with columns [article_id, entity, _crawl_date].

    Returns:
        DataFrame with columns:
            entity, first_appearance, emergence_speed, acceleration,
            total_mentions_14d, is_emerging
    """
    if entity_mentions.empty:
        logger.warning("EA-3: no entity mentions, returning empty emergence index")
        return pd.DataFrame(
            columns=["entity", "first_appearance", "emergence_speed",
                     "acceleration", "total_mentions_14d", "is_emerging"]
        )

    # Convert _crawl_date to proper date for computation
    entity_mentions = entity_mentions.copy()
    entity_mentions["date"] = pd.to_datetime(entity_mentions["_crawl_date"])

    # Find first appearance per entity — limit to top entities by mention count
    # to avoid O(N²) on 500K+ entities
    _EA3_MAX_ENTITIES = 1000
    entity_counts = entity_mentions["entity"].value_counts()
    top_entities = set(entity_counts.head(_EA3_MAX_ENTITIES).index)
    filtered_mentions = entity_mentions[entity_mentions["entity"].isin(top_entities)]

    first_appearance = filtered_mentions.groupby("entity")["date"].min().reset_index()
    first_appearance.columns = ["entity", "first_appearance"]

    logger.info("EA-3: analyzing %d entities (top %d by mention count)",
                len(first_appearance), _EA3_MAX_ENTITIES)

    rows: list[dict[str, Any]] = []

    # Pre-group by entity for O(1) lookup instead of O(N) filter per entity
    entity_groups = {
        name: group for name, group in filtered_mentions.groupby("entity")
    }

    for _, fa_row in first_appearance.iterrows():
        entity = fa_row["entity"]
        first_date = fa_row["first_appearance"]
        end_date = first_date + pd.Timedelta(days=_EA3_EMERGENCE_WINDOW_DAYS)

        # Get mentions within the emergence window
        group = entity_groups.get(entity)
        if group is None:
            continue
        entity_data = group[
            (group["date"] >= first_date) & (group["date"] < end_date)
        ]

        # Daily mention counts (fill gaps with 0)
        if entity_data.empty:
            continue

        daily_counts = (
            entity_data.groupby(entity_data["date"].dt.date)
            .size()
            .reset_index(name="count")
        )
        daily_counts.columns = ["date", "count"]

        # Build full date range and fill missing days with 0
        date_range = pd.date_range(
            start=first_date, periods=_EA3_EMERGENCE_WINDOW_DAYS, freq="D"
        )
        full_daily = pd.DataFrame({"date": date_range.date})
        full_daily = full_daily.merge(daily_counts, on="date", how="left").fillna(0)
        counts = full_daily["count"].values.astype(np.float64)

        # Cumulative mentions
        cumulative = np.cumsum(counts)
        total_14d = int(cumulative[-1]) if len(cumulative) > 0 else 0

        # Emergence speed: slope of linear fit on cumulative mentions
        if len(cumulative) >= 2:
            x = np.arange(len(cumulative), dtype=np.float64)
            coeffs = np.polyfit(x, cumulative, 1)
            emergence_speed = float(coeffs[0])
        else:
            emergence_speed = float(total_14d)

        # Acceleration: 2nd derivative of cumulative mentions
        # Using the 2nd-order polynomial fit: acceleration = 2 * a2
        if len(cumulative) >= 3:
            coeffs2 = np.polyfit(np.arange(len(cumulative), dtype=np.float64),
                                 cumulative, 2)
            acceleration = float(2.0 * coeffs2[0])
        else:
            acceleration = 0.0

        is_emerging = acceleration >= EMERGENCE_ACCELERATION_MIN

        rows.append({
            "entity": entity,
            "first_appearance": first_date.strftime("%Y-%m-%d"),
            "emergence_speed": round(emergence_speed, 6),
            "acceleration": round(acceleration, 6),
            "total_mentions_14d": total_14d,
            "is_emerging": is_emerging,
        })

    if not rows:
        logger.warning("EA-3: no emergence data computed")
        return pd.DataFrame(
            columns=["entity", "first_appearance", "emergence_speed",
                     "acceleration", "total_mentions_14d", "is_emerging"]
        )

    result_df = pd.DataFrame(rows).sort_values(
        "acceleration", ascending=False
    ).reset_index(drop=True)

    n_emerging = result_df["is_emerging"].sum()
    logger.info(
        "EA-3: computed emergence index for %d entities, %d emerging (accel >= %.3f)",
        len(result_df), n_emerging, EMERGENCE_ACCELERATION_MIN,
    )
    return result_df


# ---------------------------------------------------------------------------
# EA-4: Cross-Language Entity Reach
# ---------------------------------------------------------------------------

def _compute_cross_language_reach(
    entity_mentions: pd.DataFrame,
    min_mentions: int,
) -> pd.DataFrame:
    """Compute cross-language entity reach over time.

    For each entity, count distinct languages it appears in per week.
    Entities with < min_mentions total are excluded.

    Args:
        entity_mentions: DataFrame with columns [article_id, entity, _crawl_date, language].
        min_mentions: Minimum total mentions for inclusion.

    Returns:
        DataFrame with columns:
            entity, week, language_count, languages, total_mentions
    """
    if entity_mentions.empty or "language" not in entity_mentions.columns:
        logger.warning("EA-4: no entity mentions or language data, returning empty")
        return pd.DataFrame(
            columns=["entity", "week", "language_count", "languages", "total_mentions"]
        )

    # Filter by minimum mentions
    mention_counts = entity_mentions["entity"].value_counts()
    qualifying = set(mention_counts[mention_counts >= min_mentions].index)

    if not qualifying:
        logger.warning("EA-4: no entities with >= %d mentions", min_mentions)
        return pd.DataFrame(
            columns=["entity", "week", "language_count", "languages", "total_mentions"]
        )

    filtered = entity_mentions[entity_mentions["entity"].isin(qualifying)].copy()
    filtered["week"] = filtered["_crawl_date"].apply(_assign_week)

    rows: list[dict[str, Any]] = []

    for (entity, week), group in filtered.groupby(["entity", "week"]):
        langs = sorted(group["language"].dropna().unique().tolist())
        rows.append({
            "entity": entity,
            "week": week,
            "language_count": len(langs),
            "languages": ",".join(langs),
            "total_mentions": len(group),
        })

    if not rows:
        logger.warning("EA-4: no cross-language reach data computed")
        return pd.DataFrame(
            columns=["entity", "week", "language_count", "languages", "total_mentions"]
        )

    result_df = pd.DataFrame(rows).sort_values(
        ["entity", "week"]
    ).reset_index(drop=True)

    # Summary statistics
    max_reach = result_df.groupby("entity")["language_count"].max()
    multilingual = (max_reach > 1).sum()
    logger.info(
        "EA-4: tracked %d entities across %d weeks, %d multilingual (reach > 1)",
        result_df["entity"].nunique(),
        result_df["week"].nunique(),
        multilingual,
    )
    return result_df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_entity_analysis(
    corpus: WindowCorpus,
    output_dir: Path,
    prior_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Execute all 4 entity analytics metrics (EA-1 through EA-4).

    Loads articles and NER data from the WindowCorpus, explodes entity
    lists, then computes each metric independently. Results are saved
    as Parquet files and returned as a dict for downstream P1 validation.

    Args:
        corpus: WindowCorpus providing lazy-loaded Parquet access.
        output_dir: Directory to write output Parquet files.
        prior_metrics: Metrics from previously completed modules (unused by M3).

    Returns:
        Dict with keys:
            trajectory_types: dict[str, str] — entity -> trajectory type
            hidden_connections: dict[str, float] — 'entityA|entityB' -> jaccard
    """
    logger.info("=== M3 Entity Analytics: start ===")

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    articles_df = corpus.load_parquet(
        "articles", columns=["article_id", "source", "language", "published_at"]
    )
    ner_df = corpus.load_parquet(
        "ner", columns=["article_id", "entities_person", "entities_org",
                        "entities_location"]
    )

    # Edge case: completely empty data
    if articles_df.empty or ner_df.empty:
        logger.warning(
            "M3: insufficient data (articles=%d, ner=%d), returning empty",
            len(articles_df), len(ner_df),
        )
        _write_empty_outputs(output_dir)
        return _empty_metrics()

    # ------------------------------------------------------------------
    # Explode NER entities into long format
    # ------------------------------------------------------------------
    entity_long = _explode_entities(ner_df)

    if entity_long.empty:
        logger.warning("M3: no entities extracted from NER data, returning empty")
        _write_empty_outputs(output_dir)
        return _empty_metrics()

    # Merge _crawl_date and language from articles into entity mentions
    merge_cols = ["article_id"]
    if "_crawl_date" in articles_df.columns:
        merge_cols.append("_crawl_date")
    if "language" in articles_df.columns:
        merge_cols.append("language")

    entity_mentions = entity_long.merge(
        articles_df[merge_cols].drop_duplicates("article_id"),
        on="article_id",
        how="inner",
    )

    if "_crawl_date" not in entity_mentions.columns:
        logger.warning("M3: _crawl_date not available, using fallback")
        entity_mentions["_crawl_date"] = corpus.end_date

    logger.info(
        "M3: %d entity mentions, %d unique entities, %d articles",
        len(entity_mentions),
        entity_mentions["entity"].nunique(),
        entity_mentions["article_id"].nunique(),
    )

    # ------------------------------------------------------------------
    # EA-1: Entity Trajectory Classification
    # ------------------------------------------------------------------
    logger.info("--- EA-1: Entity Trajectory Classification ---")
    trajectories_df = _compute_trajectories(entity_mentions, INSIGHT_MIN_ENTITY_MENTIONS)

    # ------------------------------------------------------------------
    # EA-2: Hidden Connection Discovery
    # ------------------------------------------------------------------
    logger.info("--- EA-2: Hidden Connection Discovery ---")
    hidden_df = _compute_hidden_connections(
        entity_mentions, INSIGHT_MIN_ENTITY_MENTIONS, _EA2_MAX_ENTITIES
    )

    # ------------------------------------------------------------------
    # EA-3: Entity Emergence Index
    # ------------------------------------------------------------------
    logger.info("--- EA-3: Entity Emergence Index ---")
    emergence_df = _compute_emergence_index(entity_mentions)

    # ------------------------------------------------------------------
    # EA-4: Cross-Language Entity Reach
    # ------------------------------------------------------------------
    logger.info("--- EA-4: Cross-Language Entity Reach ---")
    reach_df = _compute_cross_language_reach(
        entity_mentions, INSIGHT_MIN_ENTITY_MENTIONS
    )

    # ------------------------------------------------------------------
    # Write output Parquet files
    # ------------------------------------------------------------------
    # trajectories.parquet: EA-1 trajectories + EA-3 emergence + EA-4 reach summary
    trajectories_out = _build_trajectories_output(trajectories_df, emergence_df, reach_df)
    _write_parquet(trajectories_out, output_dir / "trajectories.parquet")

    # hidden_connections.parquet: EA-2 hidden connections
    _write_parquet(hidden_df, output_dir / "hidden_connections.parquet")

    # ------------------------------------------------------------------
    # Build validation metrics dict
    # ------------------------------------------------------------------
    trajectory_types: dict[str, str] = {}
    if not trajectories_df.empty:
        trajectory_types = dict(
            zip(trajectories_df["entity"], trajectories_df["trajectory_type"])
        )

    hidden_connections: dict[str, float] = {}
    if not hidden_df.empty:
        for _, row in hidden_df.iterrows():
            pair_key = f"{row['entity_a']}|{row['entity_b']}"
            hidden_connections[pair_key] = float(row["jaccard"])

    metrics: dict[str, Any] = {
        "trajectory_types": trajectory_types,
        "hidden_connections": hidden_connections,
    }

    logger.info(
        "=== M3 Entity Analytics: complete — "
        "trajectories=%d, hidden_connections=%d, "
        "emerging=%d, multilingual=%d ===",
        len(trajectory_types),
        len(hidden_connections),
        emergence_df["is_emerging"].sum() if not emergence_df.empty else 0,
        (reach_df.groupby("entity")["language_count"].max() > 1).sum()
        if not reach_df.empty else 0,
    )

    return metrics


# ---------------------------------------------------------------------------
# Output builders
# ---------------------------------------------------------------------------

def _build_trajectories_output(
    trajectories_df: pd.DataFrame,
    emergence_df: pd.DataFrame,
    reach_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge trajectory, emergence, and reach data into a single output DataFrame.

    Args:
        trajectories_df: EA-1 trajectory classification results.
        emergence_df: EA-3 emergence index results.
        reach_df: EA-4 cross-language reach results.

    Returns:
        Merged DataFrame for trajectories.parquet output.
    """
    if trajectories_df.empty:
        # Return minimal schema even if empty
        return pd.DataFrame(
            columns=["entity", "trajectory_type", "total_mentions",
                     "num_weeks", "slope", "cv",
                     "first_appearance", "emergence_speed", "acceleration",
                     "is_emerging", "max_language_count"]
        )

    # Start with trajectories
    # Convert pagerank_series list to string for Parquet compatibility
    out = trajectories_df.copy()
    if "pagerank_series" in out.columns:
        out["pagerank_series"] = out["pagerank_series"].apply(
            lambda x: ",".join(f"{v:.8f}" for v in x) if isinstance(x, list) else ""
        )

    # Merge emergence data (EA-3)
    if not emergence_df.empty:
        emergence_cols = ["entity", "first_appearance", "emergence_speed",
                         "acceleration", "total_mentions_14d", "is_emerging"]
        existing = [c for c in emergence_cols if c in emergence_df.columns]
        out = out.merge(emergence_df[existing], on="entity", how="left")
    else:
        out["first_appearance"] = None
        out["emergence_speed"] = 0.0
        out["acceleration"] = 0.0
        out["total_mentions_14d"] = 0
        out["is_emerging"] = False

    # Merge reach summary (EA-4): max language_count per entity
    if not reach_df.empty:
        reach_summary = (
            reach_df.groupby("entity")["language_count"]
            .max()
            .reset_index()
            .rename(columns={"language_count": "max_language_count"})
        )
        out = out.merge(reach_summary, on="entity", how="left")
        out["max_language_count"] = out["max_language_count"].fillna(1).astype(int)
    else:
        out["max_language_count"] = 1

    return out


# ---------------------------------------------------------------------------
# Edge-case helpers
# ---------------------------------------------------------------------------

def _empty_metrics() -> dict[str, Any]:
    """Return the metrics dict shape with empty values."""
    return {
        "trajectory_types": {},
        "hidden_connections": {},
    }


def _write_empty_outputs(output_dir: Path) -> None:
    """Write minimal valid Parquet files when data is insufficient.

    Ensures L0 output validation passes (files exist with valid schema)
    even when there is not enough data to compute meaningful metrics.
    """
    _write_parquet(
        pd.DataFrame(
            columns=["entity", "trajectory_type", "total_mentions",
                     "num_weeks", "slope", "cv",
                     "first_appearance", "emergence_speed", "acceleration",
                     "is_emerging", "max_language_count"]
        ),
        output_dir / "trajectories.parquet",
    )
    _write_parquet(
        pd.DataFrame(
            columns=["entity_a", "entity_b", "jaccard",
                     "shared_neighbors", "neighbors_a", "neighbors_b"]
        ),
        output_dir / "hidden_connections.parquet",
    )
    logger.info("M3: wrote empty placeholder Parquet files")
