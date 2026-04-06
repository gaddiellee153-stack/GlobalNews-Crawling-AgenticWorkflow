"""M7: Synthesis — Aggregate M1-M6 results into a structured insight report.

Template Mode (default): fully deterministic, P1 compliant, no LLM calls.
Extracts top-N findings from each module, ranks by magnitude of change,
and formats as structured Markdown + JSON.

Satisfies C1 constraint (Claude API = $0) by default.

Reference: research/bigdata-insight-workflow-design.md, Gap 6 (Reflection #3).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.insights.constants import SYNTHESIS_MIN_CHANGE_THRESHOLD, SYNTHESIS_TOP_N

logger = logging.getLogger(__name__)


def run_synthesis(
    corpus,
    output_dir: Path,
    prior_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Generate insight report from M1-M6 results.

    Args:
        corpus: WindowCorpus (used for metadata only).
        output_dir: Directory to write synthesis outputs.
        prior_metrics: Dict of module results keyed by module short name
            (e.g., "crosslingual", "narrative", "entity", etc.).

    Returns:
        Empty dict (synthesis produces reports, not metrics for validation).
    """
    logger.info("--- M7: Synthesis (Template Mode) ---")

    findings: list[dict[str, Any]] = []

    # --- Extract findings from each module ---
    findings.extend(_extract_crosslingual_findings(prior_metrics.get("crosslingual", {})))
    findings.extend(_extract_narrative_findings(prior_metrics.get("narrative", {})))
    findings.extend(_extract_entity_findings(prior_metrics.get("entity", {})))
    findings.extend(_extract_temporal_findings(prior_metrics.get("temporal", {})))
    findings.extend(_extract_geopolitical_findings(prior_metrics.get("geopolitical", {})))
    findings.extend(_extract_economic_findings(prior_metrics.get("economic", {})))

    # Sort by absolute magnitude (most significant first)
    findings.sort(key=lambda f: abs(f.get("magnitude", 0)), reverse=True)
    top_findings = findings[:SYNTHESIS_TOP_N * 3]  # Keep more for the report

    # --- Generate Markdown report ---
    report_md = _generate_markdown_report(top_findings, corpus, prior_metrics)
    report_path = output_dir / "insight_report.md"
    report_path.write_text(report_md, encoding="utf-8")
    logger.info("M7: wrote insight_report.md (%d bytes)", len(report_md))

    # --- Generate structured JSON ---
    insight_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": corpus.window_days,
        "end_date": corpus.end_date,
        "total_findings": len(findings),
        "top_findings": top_findings[:SYNTHESIS_TOP_N],
        "modules_available": list(prior_metrics.keys()),
    }
    data_path = output_dir / "insight_data.json"
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(insight_data, f, indent=2, ensure_ascii=False, default=str)

    # --- Key findings for dashboard ---
    key_findings = {
        "summary_stats": _compute_summary_stats(prior_metrics),
        "top_5": top_findings[:5],
        "module_status": {k: "available" for k in prior_metrics},
    }
    kf_path = output_dir / "key_findings.json"
    with open(kf_path, "w", encoding="utf-8") as f:
        json.dump(key_findings, f, indent=2, ensure_ascii=False, default=str)

    logger.info("M7: synthesis complete — %d findings extracted", len(findings))
    return {}


# =============================================================================
# Finding extractors (one per module)
# =============================================================================

def _extract_crosslingual_findings(metrics: dict) -> list[dict]:
    """Extract top findings from M1 Cross-Lingual results."""
    findings = []

    # JSD spikes
    for pair, val in metrics.get("jsd_values", {}).items():
        if val > SYNTHESIS_MIN_CHANGE_THRESHOLD:
            findings.append({
                "module": "crosslingual",
                "metric": "CL-1_JSD",
                "description": f"Information asymmetry between {pair}: JSD = {val:.3f}",
                "magnitude": val,
                "detail": {"pair": pair, "jsd": val},
            })

    # Filter bubble (low Jaccard = high isolation)
    for pair, val in metrics.get("filter_bubble", {}).items():
        if val < 0.5:  # Low overlap = notable
            findings.append({
                "module": "crosslingual",
                "metric": "CL-4_FilterBubble",
                "description": f"Filter bubble: {pair} share only {val:.0%} of topics",
                "magnitude": 1.0 - val,  # Higher = more isolated
                "detail": {"pair": pair, "jaccard": val},
            })

    return findings


def _extract_narrative_findings(metrics: dict) -> list[dict]:
    """Extract top findings from M2 Narrative results."""
    findings = []

    # Voice dominance (high HHI = oligopoly)
    for topic, hhi in metrics.get("hhi_values", {}).items():
        if hhi > 0.25:
            findings.append({
                "module": "narrative",
                "metric": "NF-3_HHI",
                "description": f"Voice oligopoly in topic {topic}: HHI = {hhi:.3f}",
                "magnitude": hhi,
                "detail": {"topic": topic, "hhi": hhi},
            })

    # Low credibility sources
    for source, cred in metrics.get("source_credibility", {}).items():
        if cred < 0.5:
            findings.append({
                "module": "narrative",
                "metric": "NF-6_Credibility",
                "description": f"Low credibility source: {source} ({cred:.0%})",
                "magnitude": 1.0 - cred,
                "detail": {"source": source, "credibility": cred},
            })

    return findings


def _extract_entity_findings(metrics: dict) -> list[dict]:
    """Extract top findings from M3 Entity results."""
    findings = []

    for entity, ttype in metrics.get("trajectory_types", {}).items():
        if ttype == "rising_star":
            findings.append({
                "module": "entity",
                "metric": "EA-1_Trajectory",
                "description": f"Rising entity: {entity}",
                "magnitude": 0.8,
                "detail": {"entity": entity, "type": ttype},
            })
        elif ttype == "burst":
            findings.append({
                "module": "entity",
                "metric": "EA-1_Trajectory",
                "description": f"Burst entity: {entity}",
                "magnitude": 0.6,
                "detail": {"entity": entity, "type": ttype},
            })

    # Top hidden connections
    hc = metrics.get("hidden_connections", {})
    for pair, jaccard in sorted(hc.items(), key=lambda x: -x[1])[:5]:
        findings.append({
            "module": "entity",
            "metric": "EA-2_HiddenConnection",
            "description": f"Hidden connection: {pair} (Jaccard = {jaccard:.3f})",
            "magnitude": jaccard,
            "detail": {"pair": pair, "jaccard": jaccard},
        })

    return findings


def _extract_temporal_findings(metrics: dict) -> list[dict]:
    """Extract top findings from M4 Temporal results."""
    findings = []

    # Fast propagation
    for pair, lag in metrics.get("velocity_map", {}).items():
        if isinstance(lag, (int, float)) and lag < 6.0:  # < 6 hours = fast
            findings.append({
                "module": "temporal",
                "metric": "TP-2_Velocity",
                "description": f"Fast propagation {pair}: {lag:.1f}h average lag",
                "magnitude": max(0, 1.0 - lag / 24.0),  # Faster = higher magnitude
                "detail": {"pair": pair, "lag_hours": lag},
            })

    return findings


def _extract_geopolitical_findings(metrics: dict) -> list[dict]:
    """Extract top findings from M5 Geopolitical results."""
    findings = []

    # Most negative BRI
    for pair, bri in metrics.get("bri_values", {}).items():
        if bri < -0.15:
            findings.append({
                "module": "geopolitical",
                "metric": "GI-1_BRI",
                "description": f"Negative bilateral relations: {pair} (BRI = {bri:+.3f})",
                "magnitude": abs(bri),
                "detail": {"pair": pair, "bri": bri},
            })

    # Top soft power
    for country, score in sorted(
        metrics.get("soft_power", {}).items(), key=lambda x: -x[1]
    )[:5]:
        findings.append({
            "module": "geopolitical",
            "metric": "GI-2_SoftPower",
            "description": f"Soft power leader: {country} (score = {score:.3f})",
            "magnitude": score,
            "detail": {"country": country, "score": score},
        })

    # Conflict-dominant pairs
    for pair, ratio in metrics.get("conflict_cooperation", {}).items():
        if isinstance(ratio, (int, float)) and ratio > 1.0:
            findings.append({
                "module": "geopolitical",
                "metric": "GI-4_Conflict",
                "description": f"Conflict-dominant: {pair} (ratio = {ratio:.2f})",
                "magnitude": ratio,
                "detail": {"pair": pair, "ratio": ratio},
            })

    return findings


def _extract_economic_findings(metrics: dict) -> list[dict]:
    """Extract top findings from M6 Economic results."""
    findings = []

    # High EPU
    for lang, epu in metrics.get("epu_values", {}).items():
        if epu > 0.3:
            findings.append({
                "module": "economic",
                "metric": "EI-1_EPU",
                "description": f"High economic uncertainty ({lang}): EPU = {epu:.3f}",
                "magnitude": epu,
                "detail": {"language": lang, "epu": epu},
            })

    # Negative sector sentiment
    for sector, sent in metrics.get("sector_sentiment", {}).items():
        if sent < -0.05:
            findings.append({
                "module": "economic",
                "metric": "EI-2_SectorSentiment",
                "description": f"Negative sentiment in {sector}: {sent:+.3f}",
                "magnitude": abs(sent),
                "detail": {"sector": sector, "sentiment": sent},
            })

    return findings


# =============================================================================
# Report generation
# =============================================================================

def _generate_markdown_report(
    findings: list[dict],
    corpus,
    prior_metrics: dict,
) -> str:
    """Generate a structured Markdown insight report (Template Mode)."""
    lines = [
        f"# Global News Insight Brief",
        f"",
        f"- **Window**: {corpus.window_days} days ending {corpus.end_date}",
        f"- **Data**: {corpus.total_available_days} days available",
        f"- **Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"- **Modules**: {', '.join(prior_metrics.keys()) or 'none'}",
        f"- **Total findings**: {len(findings)}",
        f"",
    ]

    # Group findings by module
    by_module: dict[str, list[dict]] = {}
    for f in findings:
        mod = f.get("module", "unknown")
        by_module.setdefault(mod, []).append(f)

    module_titles = {
        "crosslingual": "Cross-Lingual Asymmetry",
        "narrative": "Narrative & Framing",
        "entity": "Entity Analytics",
        "temporal": "Temporal Patterns",
        "geopolitical": "Geopolitical Index",
        "economic": "Economic Intelligence",
    }

    for mod_key in ["crosslingual", "geopolitical", "economic", "narrative", "entity", "temporal"]:
        mod_findings = by_module.get(mod_key, [])
        title = module_titles.get(mod_key, mod_key)
        lines.append(f"## {title}")
        lines.append("")

        if not mod_findings:
            lines.append("_No notable findings in this module._")
            lines.append("")
            continue

        for f in mod_findings[:SYNTHESIS_TOP_N]:
            lines.append(f"- **[{f['metric']}]** {f['description']}")

        lines.append("")

    return "\n".join(lines)


def _compute_summary_stats(prior_metrics: dict) -> dict:
    """Compute aggregate summary statistics across all modules."""
    stats = {}

    cl = prior_metrics.get("crosslingual", {})
    jsd = cl.get("jsd_values", {})
    if jsd:
        stats["mean_jsd"] = sum(jsd.values()) / len(jsd)
        stats["max_jsd_pair"] = max(jsd, key=jsd.get) if jsd else None

    gi = prior_metrics.get("geopolitical", {})
    bri = gi.get("bri_values", {})
    if bri:
        stats["most_negative_pair"] = min(bri, key=bri.get) if bri else None
        stats["most_negative_bri"] = min(bri.values()) if bri else None

    ei = prior_metrics.get("economic", {})
    epu = ei.get("epu_values", {})
    if epu:
        stats["max_epu_lang"] = max(epu, key=epu.get) if epu else None
        stats["max_epu"] = max(epu.values()) if epu else None

    return stats
