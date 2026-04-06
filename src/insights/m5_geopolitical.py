"""M5: Geopolitical Analytics — Bilateral Relations, Soft Power, Agenda-Setting, Conflict Spectrum.

Four purely arithmetic (Type A) / rule-based (Type B) metrics:

    GI-1  Bilateral Relations Index (BRI)
          Mean sentiment of articles co-mentioning a country pair,
          decomposed by 8 Plutchik emotions.

    GI-2  Soft Power Score
          Composite index per country: visibility + sentiment +
          STEEPS frame diversity (Shannon entropy) + co-mention centrality.

    GI-3  Agenda-Setting Power
          Which language community publishes first on each topic.
          Per-language "leader score" = fraction of topics where
          that language was chronologically first.

    GI-4  Conflict-Cooperation Spectrum
          (anger + fear) / (trust + anticipation + 1e-6) for each
          country pair.  >1 = conflict-dominant, <1 = cooperation.

No LLM.  No ML inference.  All deterministic arithmetic.

Outputs (Parquet, ZSTD):
    bilateral_index.parquet   — GI-1 + GI-4 per pair
    soft_power.parquet        — GI-2 per country
    agenda_influence.parquet  — GI-3 per language

Reference: research/bigdata-insight-workflow-design.md §M5
"""

from __future__ import annotations

import logging
import math
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.constants import (
    INSIGHT_MIN_COUNTRY_PAIR_ARTICLES,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)
from src.insights.constants import BRI_BASELINE_DAYS, SOFT_POWER_WEIGHTS
from src.insights.window_assembler import WindowCorpus

logger = logging.getLogger(__name__)

# =============================================================================
# Country Name / Demonym -> ISO-alpha2 mapping
# =============================================================================
# Covers English, Korean, Japanese, and common variations for the most
# frequently mentioned countries in a multilingual news corpus.

COUNTRY_NAMES: dict[str, str] = {
    # --- United States ---
    "United States": "US",
    "United States of America": "US",
    "the United States": "US",
    "USA": "US",
    "US": "US",
    "U.S.": "US",
    "U.S.A.": "US",
    "America": "US",
    "American": "US",
    "Washington": "US",
    "미국": "US",
    "アメリカ": "US",
    "米国": "US",
    # --- China ---
    "China": "CN",
    "Chinese": "CN",
    "PRC": "CN",
    "People's Republic of China": "CN",
    "중국": "CN",
    "中国": "CN",
    "中國": "CN",
    # --- Japan ---
    "Japan": "JP",
    "Japanese": "JP",
    "일본": "JP",
    "日本": "JP",
    # --- South Korea ---
    "South Korea": "KR",
    "Korea": "KR",
    "Republic of Korea": "KR",
    "Korean": "KR",
    "한국": "KR",
    "대한민국": "KR",
    "韓国": "KR",
    # --- North Korea ---
    "North Korea": "KP",
    "DPRK": "KP",
    "북한": "KP",
    "조선": "KP",
    "北朝鮮": "KP",
    # --- Russia ---
    "Russia": "RU",
    "Russian": "RU",
    "Russian Federation": "RU",
    "러시아": "RU",
    "ロシア": "RU",
    "Россия": "RU",
    # --- United Kingdom ---
    "United Kingdom": "GB",
    "UK": "GB",
    "Britain": "GB",
    "British": "GB",
    "Great Britain": "GB",
    "England": "GB",
    "영국": "GB",
    "イギリス": "GB",
    "英国": "GB",
    # --- Germany ---
    "Germany": "DE",
    "German": "DE",
    "Deutschland": "DE",
    "독일": "DE",
    "ドイツ": "DE",
    # --- France ---
    "France": "FR",
    "French": "FR",
    "프랑스": "FR",
    "フランス": "FR",
    # --- India ---
    "India": "IN",
    "Indian": "IN",
    "인도": "IN",
    "インド": "IN",
    # --- Taiwan ---
    "Taiwan": "TW",
    "Taiwanese": "TW",
    "대만": "TW",
    "台湾": "TW",
    "台灣": "TW",
    # --- Iran ---
    "Iran": "IR",
    "Iranian": "IR",
    "Tehran": "IR",
    "이란": "IR",
    "イラン": "IR",
    # --- Israel ---
    "Israel": "IL",
    "Israeli": "IL",
    "이스라엘": "IL",
    "イスラエル": "IL",
    # --- Ukraine ---
    "Ukraine": "UA",
    "Ukrainian": "UA",
    "우크라이나": "UA",
    "ウクライナ": "UA",
    # --- Canada ---
    "Canada": "CA",
    "Canadian": "CA",
    "캐나다": "CA",
    "カナダ": "CA",
    # --- Australia ---
    "Australia": "AU",
    "Australian": "AU",
    "호주": "AU",
    "オーストラリア": "AU",
    # --- Brazil ---
    "Brazil": "BR",
    "Brazilian": "BR",
    "브라질": "BR",
    "ブラジル": "BR",
    # --- Mexico ---
    "Mexico": "MX",
    "Mexican": "MX",
    "멕시코": "MX",
    "メキシコ": "MX",
    # --- Italy ---
    "Italy": "IT",
    "Italian": "IT",
    "이탈리아": "IT",
    "イタリア": "IT",
    # --- Spain ---
    "Spain": "ES",
    "Spanish": "ES",
    "스페인": "ES",
    "スペイン": "ES",
    # --- Turkey ---
    "Turkey": "TR",
    "Turkish": "TR",
    "Turkiye": "TR",
    "터키": "TR",
    "トルコ": "TR",
    # --- Saudi Arabia ---
    "Saudi Arabia": "SA",
    "Saudi": "SA",
    "사우디아라비아": "SA",
    "사우디": "SA",
    "サウジアラビア": "SA",
    # --- Indonesia ---
    "Indonesia": "ID",
    "Indonesian": "ID",
    "인도네시아": "ID",
    "インドネシア": "ID",
    # --- Poland ---
    "Poland": "PL",
    "Polish": "PL",
    "폴란드": "PL",
    "ポーランド": "PL",
    # --- Philippines ---
    "Philippines": "PH",
    "Filipino": "PH",
    "필리핀": "PH",
    "フィリピン": "PH",
    # --- Vietnam ---
    "Vietnam": "VN",
    "Vietnamese": "VN",
    "베트남": "VN",
    "ベトナム": "VN",
    # --- Singapore ---
    "Singapore": "SG",
    "Singaporean": "SG",
    "싱가포르": "SG",
    "シンガポール": "SG",
    # --- Egypt ---
    "Egypt": "EG",
    "Egyptian": "EG",
    "이집트": "EG",
    "エジプト": "EG",
    # --- South Africa ---
    "South Africa": "ZA",
    "남아프리카": "ZA",
    "南アフリカ": "ZA",
    # --- Netherlands ---
    "Netherlands": "NL",
    "Dutch": "NL",
    "네덜란드": "NL",
    "オランダ": "NL",
    # --- Sweden ---
    "Sweden": "SE",
    "Swedish": "SE",
    "스웨덴": "SE",
    # --- Norway ---
    "Norway": "NO",
    "Norwegian": "NO",
    "노르웨이": "NO",
    # --- Pakistan ---
    "Pakistan": "PK",
    "Pakistani": "PK",
    "파키스탄": "PK",
    # --- Syria ---
    "Syria": "SY",
    "Syrian": "SY",
    "시리아": "SY",
    # --- Palestine ---
    "Palestine": "PS",
    "Palestinian": "PS",
    "팔레스타인": "PS",
    # --- EU (supranational, treated as entity) ---
    "European Union": "EU",
    "EU": "EU",
    "유럽연합": "EU",
}

# Plutchik 8 emotion columns as stored in article_analysis.parquet
PLUTCHIK_COLUMNS = [
    "emotion_joy",
    "emotion_trust",
    "emotion_fear",
    "emotion_surprise",
    "emotion_sadness",
    "emotion_anger",
    "emotion_disgust",
    "emotion_anticipation",
]

# STEEPS category codes (from stage3_article_analysis.py STEEPS_CODE_MAP)
STEEPS_CODES = {"S", "T", "E", "En", "P", "Se"}


# =============================================================================
# Helper functions
# =============================================================================


def _resolve_countries(location_entities: Any) -> set[str]:
    """Extract ISO country codes from a NER entities_location field.

    The entities_location column is a list of location strings extracted by
    the multilingual NER model (Stage 2).  We map each string to an ISO code
    using the COUNTRY_NAMES dictionary.

    Args:
        location_entities: A list of strings, or a single string, or NaN/None.

    Returns:
        Set of ISO-alpha2 country codes found.
    """
    if location_entities is None:
        return set()

    # Handle the case where pandas reads a list column as actual list
    if isinstance(location_entities, str):
        # Could be a stringified list like "['Japan', 'China']"
        if location_entities.startswith("["):
            try:
                import ast
                location_entities = ast.literal_eval(location_entities)
            except (ValueError, SyntaxError):
                location_entities = [location_entities]
        else:
            location_entities = [location_entities]
    elif hasattr(location_entities, "__iter__"):
        # numpy ndarray or other iterable
        location_entities = list(location_entities)
    else:
        return set()

    codes: set[str] = set()
    for loc in location_entities:
        if not isinstance(loc, str) or not loc.strip():
            continue
        loc_clean = loc.strip()
        code = COUNTRY_NAMES.get(loc_clean)
        if code is not None:
            codes.add(code)
    return codes


def _make_pair_key(a: str, b: str) -> str:
    """Create a canonical sorted pair key: 'CN-US' (alphabetical)."""
    return "-".join(sorted([a, b]))


def _shannon_entropy(counts: dict[str, int]) -> float:
    """Compute Shannon entropy (base 2) from a frequency dict.

    Returns 0.0 if the distribution is empty or has a single category.
    """
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    entropy = 0.0
    for c in counts.values():
        if c > 0:
            p = c / total
            entropy -= p * math.log2(p)
    return entropy


# =============================================================================
# GI-1: Bilateral Relations Index
# =============================================================================


def _compute_bri(
    articles_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    ner_df: pd.DataFrame,
    min_pair_articles: int,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Compute Bilateral Relations Index for every qualifying country pair.

    For each article, resolves country mentions.  For every pair of countries
    co-mentioned in the same article, records the article's sentiment_score
    and 8 Plutchik emotion scores.  The BRI is the mean sentiment_score
    over articles mentioning both countries.

    Args:
        articles_df: Articles with article_id.
        analysis_df: Article analysis with sentiment_score and emotion columns.
        ner_df: NER output with entities_location.
        min_pair_articles: Minimum articles for a pair to be included.

    Returns:
        Tuple of (DataFrame for Parquet output, dict of pair->BRI for validation).
    """
    # Merge analysis + NER on article_id
    merged = analysis_df.merge(ner_df[["article_id", "entities_location"]], on="article_id", how="inner")

    # Resolve countries per article
    article_countries: dict[str, set[str]] = {}
    for _, row in merged.iterrows():
        aid = row["article_id"]
        countries = _resolve_countries(row.get("entities_location"))
        if len(countries) >= 2:
            article_countries[aid] = countries

    if not article_countries:
        logger.warning("GI-1: No articles with 2+ country mentions found")
        return pd.DataFrame(), {}

    # Build pair -> list of article_ids
    pair_articles: dict[str, list[str]] = defaultdict(list)
    for aid, countries in article_countries.items():
        for c1, c2 in combinations(sorted(countries), 2):
            pair_key = _make_pair_key(c1, c2)
            pair_articles[pair_key].append(aid)

    # Index analysis_df by article_id for fast lookup
    analysis_indexed = merged.set_index("article_id")

    # Compute BRI + emotion profile for each qualifying pair
    rows = []
    bri_values: dict[str, float] = {}

    emotion_cols_available = [c for c in PLUTCHIK_COLUMNS if c in analysis_indexed.columns]

    for pair_key, aids in pair_articles.items():
        unique_aids = list(set(aids))
        if len(unique_aids) < min_pair_articles:
            continue

        subset = analysis_indexed.loc[
            analysis_indexed.index.isin(unique_aids)
        ]

        if subset.empty:
            continue

        # Mean sentiment
        mean_sentiment = float(subset["sentiment_score"].mean())

        # Emotion profile: mean of each Plutchik dimension
        emotion_profile: dict[str, float] = {}
        for ecol in emotion_cols_available:
            col_data = pd.to_numeric(subset[ecol], errors="coerce")
            emotion_profile[ecol] = float(col_data.mean()) if not col_data.isna().all() else 0.0

        country_a, country_b = pair_key.split("-")
        bri_values[pair_key] = mean_sentiment

        row = {
            "country_a": country_a,
            "country_b": country_b,
            "pair_key": pair_key,
            "article_count": len(unique_aids),
            "bri_score": mean_sentiment,
        }
        row.update(emotion_profile)
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("article_count", ascending=False).reset_index(drop=True)

    logger.info(
        "GI-1 BRI: %d qualifying pairs from %d total pairs (%d multi-country articles)",
        len(rows), len(pair_articles), len(article_countries),
    )
    return df, bri_values


# =============================================================================
# GI-2: Soft Power Score
# =============================================================================


def _compute_soft_power(
    articles_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    ner_df: pd.DataFrame,
    weights: dict[str, float],
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Compute Soft Power composite index per country.

    Components:
        visibility    = articles_mentioning_country / total_articles
        sentiment     = mean(sentiment_score) of mentioning articles,
                        normalized to [0, 1] via (x + 1) / 2
        frame_diversity = Shannon entropy of STEEPS distribution in
                          mentioning articles, normalized by log2(6)
        centrality    = count(unique co-mentioned countries), normalized
                        by max centrality across all countries

    Composite = weighted sum using SOFT_POWER_WEIGHTS.

    Args:
        articles_df: Articles DataFrame.
        analysis_df: Analysis DataFrame with sentiment_score, steeps_category.
        ner_df: NER DataFrame with entities_location.
        weights: Component weight dict (SOFT_POWER_WEIGHTS).

    Returns:
        Tuple of (DataFrame for Parquet, dict of country->score for validation).
    """
    total_articles = len(articles_df)
    if total_articles == 0:
        logger.warning("GI-2: No articles for soft power computation")
        return pd.DataFrame(), {}

    # Merge analysis + NER
    merged = analysis_df.merge(
        ner_df[["article_id", "entities_location"]], on="article_id", how="inner"
    )

    # Resolve countries per article
    article_countries: dict[str, set[str]] = {}
    for _, row in merged.iterrows():
        aid = row["article_id"]
        countries = _resolve_countries(row.get("entities_location"))
        if countries:
            article_countries[aid] = countries

    if not article_countries:
        logger.warning("GI-2: No articles with country mentions found")
        return pd.DataFrame(), {}

    # Invert: country -> set of article_ids
    country_articles: dict[str, set[str]] = defaultdict(set)
    for aid, countries in article_countries.items():
        for c in countries:
            country_articles[c].add(aid)

    # Index merged data by article_id
    analysis_indexed = merged.set_index("article_id")

    # --- Compute raw components per country ---
    raw_visibility: dict[str, float] = {}
    raw_sentiment: dict[str, float] = {}
    raw_frame_div: dict[str, float] = {}
    raw_centrality: dict[str, int] = {}

    for country, aids in country_articles.items():
        aids_list = list(aids)
        subset = analysis_indexed.loc[analysis_indexed.index.isin(aids_list)]
        if subset.empty:
            continue

        # Visibility: fraction of total articles
        raw_visibility[country] = len(aids) / total_articles

        # Sentiment: mean sentiment_score
        raw_sentiment[country] = float(subset["sentiment_score"].mean())

        # Frame diversity: Shannon entropy of STEEPS categories
        steeps_counts: dict[str, int] = {}
        if "steeps_category" in subset.columns:
            for cat in subset["steeps_category"].dropna():
                cat_str = str(cat).strip()
                if cat_str in STEEPS_CODES:
                    steeps_counts[cat_str] = steeps_counts.get(cat_str, 0) + 1
        raw_frame_div[country] = _shannon_entropy(steeps_counts)

        # Centrality: count of unique OTHER countries co-mentioned
        co_countries: set[str] = set()
        for aid in aids:
            if aid in article_countries:
                co_countries.update(article_countries[aid])
        co_countries.discard(country)
        raw_centrality[country] = len(co_countries)

    if not raw_visibility:
        return pd.DataFrame(), {}

    # --- Normalize components to [0, 1] ---
    all_countries = list(raw_visibility.keys())

    # Visibility: already a fraction [0, 1], but normalize by max for relative ranking
    max_vis = max(raw_visibility.values()) if raw_visibility else 1.0
    norm_visibility = {c: raw_visibility.get(c, 0.0) / max_vis if max_vis > 0 else 0.0
                       for c in all_countries}

    # Sentiment: transform from [-1, 1] to [0, 1]
    norm_sentiment = {c: (raw_sentiment.get(c, 0.0) + 1.0) / 2.0
                      for c in all_countries}

    # Frame diversity: normalize by max possible entropy (log2(6) for 6 STEEPS codes)
    max_entropy = math.log2(len(STEEPS_CODES)) if len(STEEPS_CODES) > 1 else 1.0
    norm_frame_div = {c: min(1.0, raw_frame_div.get(c, 0.0) / max_entropy)
                      for c in all_countries}

    # Centrality: normalize by max centrality
    max_cent = max(raw_centrality.values()) if raw_centrality else 1
    norm_centrality = {c: raw_centrality.get(c, 0) / max_cent if max_cent > 0 else 0.0
                       for c in all_countries}

    # --- Composite score ---
    w_vis = weights.get("visibility", 0.25)
    w_sen = weights.get("sentiment", 0.30)
    w_frm = weights.get("frame_diversity", 0.20)
    w_cen = weights.get("centrality", 0.25)

    rows = []
    soft_power_values: dict[str, float] = {}

    for country in all_countries:
        composite = (
            w_vis * norm_visibility[country]
            + w_sen * norm_sentiment[country]
            + w_frm * norm_frame_div[country]
            + w_cen * norm_centrality[country]
        )
        soft_power_values[country] = composite

        rows.append({
            "country": country,
            "soft_power_score": composite,
            "visibility": norm_visibility[country],
            "visibility_raw": raw_visibility[country],
            "sentiment": norm_sentiment[country],
            "sentiment_raw": raw_sentiment[country],
            "frame_diversity": norm_frame_div[country],
            "frame_diversity_raw": raw_frame_div[country],
            "centrality": norm_centrality[country],
            "centrality_raw": raw_centrality.get(country, 0),
            "article_count": len(country_articles.get(country, set())),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("soft_power_score", ascending=False).reset_index(drop=True)

    logger.info("GI-2 Soft Power: %d countries scored", len(rows))
    return df, soft_power_values


# =============================================================================
# GI-3: Agenda-Setting Power
# =============================================================================


def _compute_agenda_setting(
    articles_df: pd.DataFrame,
    topics_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Compute Agenda-Setting Power per language community.

    For each topic, finds the FIRST published_at timestamp per language.
    The language that publishes first on a topic is the "leader" for that topic.
    The leader score for a language = fraction of topics where it was first.

    Args:
        articles_df: Articles with article_id, language, published_at.
        topics_df: Topics with article_id, topic_id, topic_label, published_at.

    Returns:
        Tuple of (DataFrame for Parquet, dict of language->leader_score for validation).
    """
    if topics_df.empty or articles_df.empty:
        logger.warning("GI-3: Empty input data for agenda-setting analysis")
        return pd.DataFrame(), {}

    # Join topics with articles to get language
    # topics.parquet may have published_at; use articles.published_at as canonical
    topic_with_lang = topics_df.merge(
        articles_df[["article_id", "language", "published_at"]],
        on="article_id",
        how="inner",
        suffixes=("_topic", "_article"),
    )

    # Determine which published_at column to use
    if "published_at_article" in topic_with_lang.columns:
        pub_col = "published_at_article"
    elif "published_at" in topic_with_lang.columns:
        pub_col = "published_at"
    else:
        logger.warning("GI-3: No published_at column found after merge")
        return pd.DataFrame(), {}

    # Parse timestamps
    topic_with_lang["_pub_ts"] = pd.to_datetime(
        topic_with_lang[pub_col], errors="coerce", utc=True
    )
    topic_with_lang = topic_with_lang.dropna(subset=["_pub_ts", "language", "topic_id"])

    if topic_with_lang.empty:
        logger.warning("GI-3: No valid topic+language+timestamp records")
        return pd.DataFrame(), {}

    # For each (topic_id, language), find the earliest timestamp
    first_per_topic_lang = (
        topic_with_lang
        .groupby(["topic_id", "language"])["_pub_ts"]
        .min()
        .reset_index()
    )

    # For each topic_id, find which language was first
    topic_ids = first_per_topic_lang["topic_id"].unique()
    leader_counts: Counter[str] = Counter()
    topic_count = 0
    detail_rows = []

    for tid in topic_ids:
        topic_data = first_per_topic_lang[first_per_topic_lang["topic_id"] == tid]
        if topic_data.empty:
            continue

        # Need at least 2 languages to have a meaningful comparison
        if len(topic_data) < 2:
            continue

        topic_count += 1
        earliest_idx = topic_data["_pub_ts"].idxmin()
        leader_lang = topic_data.loc[earliest_idx, "language"]
        leader_counts[leader_lang] += 1

        # Get topic_label if available
        topic_label_candidates = topics_df.loc[
            topics_df["topic_id"] == tid, "topic_label"
        ]
        topic_label = (
            str(topic_label_candidates.iloc[0])
            if not topic_label_candidates.empty
            else str(tid)
        )

        # Compute lags from the leader
        leader_ts = topic_data.loc[earliest_idx, "_pub_ts"]
        for _, row in topic_data.iterrows():
            lag_hours = (row["_pub_ts"] - leader_ts).total_seconds() / 3600.0
            detail_rows.append({
                "topic_id": tid,
                "topic_label": topic_label,
                "language": row["language"],
                "first_published_at": row["_pub_ts"].isoformat(),
                "lag_hours_from_leader": round(lag_hours, 2),
                "is_leader": row["language"] == leader_lang,
            })

    # Compute leader scores
    agenda_values: dict[str, float] = {}
    if topic_count > 0:
        for lang, count in leader_counts.items():
            agenda_values[lang] = count / topic_count

    # Build summary rows (per language) + detail rows
    summary_rows = []
    all_langs = set()
    for row in detail_rows:
        all_langs.add(row["language"])

    for lang in sorted(all_langs):
        leader_score = agenda_values.get(lang, 0.0)
        topics_led = leader_counts.get(lang, 0)
        # Compute mean lag when NOT leader
        lang_lags = [
            r["lag_hours_from_leader"]
            for r in detail_rows
            if r["language"] == lang and not r["is_leader"]
        ]
        mean_follower_lag = sum(lang_lags) / len(lang_lags) if lang_lags else 0.0

        summary_rows.append({
            "language": lang,
            "leader_score": leader_score,
            "topics_led": topics_led,
            "total_topics_compared": topic_count,
            "mean_follower_lag_hours": round(mean_follower_lag, 2),
        })

    df = pd.DataFrame(summary_rows)
    if not df.empty:
        df = df.sort_values("leader_score", ascending=False).reset_index(drop=True)

    logger.info(
        "GI-3 Agenda-Setting: %d languages, %d topics compared, leaders=%s",
        len(summary_rows), topic_count,
        dict(leader_counts.most_common(5)),
    )
    return df, agenda_values


# =============================================================================
# GI-4: Conflict-Cooperation Spectrum
# =============================================================================


def _compute_conflict_cooperation(
    bri_df: pd.DataFrame,
) -> dict[str, float]:
    """Compute conflict-cooperation ratio per country pair from BRI data.

    ratio = (mean_anger + mean_fear) / (mean_trust + mean_anticipation + 1e-6)

    ratio > 1.0 -> conflict-dominant
    ratio < 1.0 -> cooperation-dominant

    This metric is computed directly from the BRI DataFrame which already
    contains the per-pair mean emotion scores.

    Args:
        bri_df: DataFrame from GI-1 with emotion columns per pair.

    Returns:
        Dict of pair_key -> conflict/cooperation ratio.
    """
    if bri_df.empty:
        logger.warning("GI-4: Empty BRI data, no conflict-cooperation ratios")
        return {}

    ratios: dict[str, float] = {}
    for _, row in bri_df.iterrows():
        pair_key = row.get("pair_key", "")
        if not pair_key:
            continue

        anger = float(row.get("emotion_anger", 0.0) or 0.0)
        fear = float(row.get("emotion_fear", 0.0) or 0.0)
        trust = float(row.get("emotion_trust", 0.0) or 0.0)
        anticipation = float(row.get("emotion_anticipation", 0.0) or 0.0)

        ratio = (anger + fear) / (trust + anticipation + 1e-6)
        ratios[pair_key] = round(ratio, 6)

    logger.info(
        "GI-4 Conflict-Cooperation: %d pairs, "
        "conflict-dominant=%d, cooperation-dominant=%d",
        len(ratios),
        sum(1 for r in ratios.values() if r > 1.0),
        sum(1 for r in ratios.values() if r <= 1.0),
    )
    return ratios


# =============================================================================
# Module entry point
# =============================================================================


def run_geopolitical_analysis(
    corpus: WindowCorpus,
    output_dir: Path,
    prior_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Execute M5 Geopolitical Analytics: BRI, Soft Power, Agenda-Setting, Conflict Spectrum.

    All metrics are Type A (pure arithmetic) or Type B (rule-based).
    No LLM, no ML inference.

    Args:
        corpus: WindowCorpus providing lazy-loaded multi-date Parquet access.
        output_dir: Directory to write output Parquet files.
        prior_metrics: Metrics from previously completed modules (M1-M4).

    Returns:
        Dict with keys: bri_values, soft_power, agenda_setting, conflict_cooperation.
        Used by P1 validators and downstream M7 synthesis.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Load data (column-selective for memory efficiency) ---
    logger.info("M5: Loading corpus data...")

    articles_df = corpus.load_parquet(
        "articles",
        columns=["article_id", "source", "language", "published_at"],
    )
    analysis_df = corpus.load_parquet(
        "article_analysis",
        columns=[
            "article_id", "sentiment_score", "steeps_category",
            "emotion_joy", "emotion_trust", "emotion_fear",
            "emotion_surprise", "emotion_sadness", "emotion_anger",
            "emotion_disgust", "emotion_anticipation",
        ],
    )
    ner_df = corpus.load_parquet(
        "ner",
        columns=["article_id", "entities_location"],
    )
    topics_df = corpus.load_parquet(
        "topics",
        columns=["article_id", "topic_id", "topic_label", "published_at"],
    )

    logger.info(
        "M5: Data loaded — articles=%d, analysis=%d, ner=%d, topics=%d",
        len(articles_df), len(analysis_df), len(ner_df), len(topics_df),
    )

    # --- GI-1: Bilateral Relations Index ---
    bri_df, bri_values = _compute_bri(
        articles_df, analysis_df, ner_df,
        min_pair_articles=INSIGHT_MIN_COUNTRY_PAIR_ARTICLES,
    )

    # --- GI-4: Conflict-Cooperation Spectrum (depends on GI-1 output) ---
    conflict_coop = _compute_conflict_cooperation(bri_df)

    # Append conflict-cooperation ratio to BRI DataFrame
    if not bri_df.empty and conflict_coop:
        bri_df["conflict_cooperation_ratio"] = bri_df["pair_key"].map(conflict_coop)
        bri_df["spectrum_label"] = bri_df["conflict_cooperation_ratio"].apply(
            lambda r: "conflict" if pd.notna(r) and r > 1.0 else "cooperation"
        )

    # --- GI-2: Soft Power Score ---
    soft_power_df, soft_power_values = _compute_soft_power(
        articles_df, analysis_df, ner_df,
        weights=SOFT_POWER_WEIGHTS,
    )

    # --- GI-3: Agenda-Setting Power ---
    agenda_df, agenda_values = _compute_agenda_setting(articles_df, topics_df)

    # --- Save outputs as Parquet with ZSTD compression ---
    pq_kwargs = {
        "compression": PARQUET_COMPRESSION,
        "compression_level": PARQUET_COMPRESSION_LEVEL,
    }

    # bilateral_index.parquet (GI-1 + GI-4)
    bri_path = output_dir / "bilateral_index.parquet"
    if not bri_df.empty:
        bri_df.to_parquet(bri_path, index=False, **pq_kwargs)
        logger.info("Saved %s (%d rows)", bri_path.name, len(bri_df))
    else:
        # Write empty DataFrame with schema to satisfy L0 validation
        _write_empty_parquet(bri_path, _BRI_SCHEMA_COLS, pq_kwargs)
        logger.warning("GI-1: No qualifying pairs — wrote empty %s", bri_path.name)

    # soft_power.parquet (GI-2)
    sp_path = output_dir / "soft_power.parquet"
    if not soft_power_df.empty:
        soft_power_df.to_parquet(sp_path, index=False, **pq_kwargs)
        logger.info("Saved %s (%d rows)", sp_path.name, len(soft_power_df))
    else:
        _write_empty_parquet(sp_path, _SP_SCHEMA_COLS, pq_kwargs)
        logger.warning("GI-2: No countries scored — wrote empty %s", sp_path.name)

    # agenda_influence.parquet (GI-3)
    agenda_path = output_dir / "agenda_influence.parquet"
    if not agenda_df.empty:
        agenda_df.to_parquet(agenda_path, index=False, **pq_kwargs)
        logger.info("Saved %s (%d rows)", agenda_path.name, len(agenda_df))
    else:
        _write_empty_parquet(agenda_path, _AGENDA_SCHEMA_COLS, pq_kwargs)
        logger.warning("GI-3: No agenda data — wrote empty %s", agenda_path.name)

    # --- Return metrics dict for P1 validation and M7 synthesis ---
    metrics = {
        "bri_values": bri_values,
        "soft_power": soft_power_values,
        "agenda_setting": agenda_values,
        "conflict_cooperation": conflict_coop,
    }

    logger.info(
        "M5 Geopolitical complete: "
        "%d BRI pairs, %d countries (soft power), %d languages (agenda), "
        "%d conflict-coop ratios",
        len(bri_values), len(soft_power_values),
        len(agenda_values), len(conflict_coop),
    )

    return metrics


# =============================================================================
# Empty Parquet schema helpers (for L0 validation when data is insufficient)
# =============================================================================

_BRI_SCHEMA_COLS = {
    "country_a": "str",
    "country_b": "str",
    "pair_key": "str",
    "article_count": "int",
    "bri_score": "float",
    "conflict_cooperation_ratio": "float",
    "spectrum_label": "str",
}

_SP_SCHEMA_COLS = {
    "country": "str",
    "soft_power_score": "float",
    "visibility": "float",
    "sentiment": "float",
    "frame_diversity": "float",
    "centrality": "float",
    "article_count": "int",
}

_AGENDA_SCHEMA_COLS = {
    "language": "str",
    "leader_score": "float",
    "topics_led": "int",
    "total_topics_compared": "int",
    "mean_follower_lag_hours": "float",
}


def _write_empty_parquet(
    path: Path,
    schema: dict[str, str],
    pq_kwargs: dict[str, Any],
) -> None:
    """Write an empty Parquet file with the correct column schema.

    This ensures L0 Anti-Skip Guard (file exists + min size) can be satisfied
    even when data is insufficient.  Downstream consumers see an empty
    DataFrame with the correct dtypes.
    """
    dtype_map = {"str": "object", "int": "int64", "float": "float64"}
    df = pd.DataFrame({
        col: pd.Series(dtype=dtype_map.get(dtype, dtype))
        for col, dtype in schema.items()
    })
    df.to_parquet(path, index=False, **pq_kwargs)
