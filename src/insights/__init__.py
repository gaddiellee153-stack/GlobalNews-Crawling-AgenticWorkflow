"""Insight Pipeline — Big Data Analytics for multilingual news corpus.

Workflow B: Consumes Stage 1-4 outputs (articles, embeddings, NER,
sentiment/STEEPS, topics, networks) across multiple dates to produce
structural insights impossible from single-day analysis.

7 modules (M1-M7), 27 metrics, 92% deterministic Python (P1).

Usage:
    .venv/bin/python main.py --mode insight --window 30 --end-date 2026-04-05
"""
