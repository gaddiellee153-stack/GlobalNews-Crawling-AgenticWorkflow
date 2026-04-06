# Big Data Insight Analytics — Dual Workflow Design

> **Status**: Draft v3 — 3회 Critical Reflection 완료. 구현 승인 대기 중.
> **Reflections**: #1 구조적 결함 (Appendix A) | #2 할루시네이션 봉쇄 (Appendix B) | #3 설계 결함 최종 (Appendix C)
> **Created**: 2026-04-06
> **Purpose**: 기존 크롤링+분석 워크플로우(Workflow A)에서 빅데이터 통찰 분석(Workflow B)을 분리하는 듀얼 워크플로우 설계안

---

## 1. Background & Motivation

### 1.1 세 워크플로우의 역할 분담

| 워크플로우 | 질문 | 성격 |
|-----------|------|------|
| **환경스캐닝** (별도 존재) | "무엇이 변하고 있는가?" (What's changing?) | 신호 탐지 |
| **시나리오/시뮬레이션** (별도 존재) | "어떻게 전개될 수 있는가?" (What could happen?) | 미래 경로 탐색 |
| **빅데이터 통찰 분석** (본 설계) | "세상의 구조가 어떤 상태인가?" (What IS the structure?) | 정량적 X-ray |

### 1.2 빅데이터 통찰의 고유 가치

다른 두 워크플로우가 해석할 수 없는 **구조적 맥락**을 공급한다.

- 환경스캐닝이 "AI 규제가 부상"을 탐지해도, **어느 나라가 선행하는지, 프레이밍이 어떻게 다른지, 누구의 목소리가 지배하는지**는 빅데이터 분석만이 답할 수 있다.
- 시나리오 워크플로우에 **데이터 기반 driving forces, critical uncertainties** 입력을 제공한다.

### 1.3 빅데이터이기 때문에 가능한 것

50건/일의 정성적 읽기로는 절대 얻을 수 없는 통찰. **수천 건 × 13개 언어 × 수개월 축적**이 있어야만 드러나는 패턴.

| 최소 데이터 요건 | 통찰 유형 |
|----------------|---------|
| 100건/언어/일 × 13언어 | 언어 간 정보 비대칭 측정 |
| 50,000건/30일 | 정보 흐름 위상(누가 누구를 베끼는가) |
| 30일 × 50회 이상 언급/엔티티 | 엔티티 궤적 예측 |
| 20건/국가쌍/주 | 양자관계 지수의 신뢰 구간 확보 |
| 365일 누적 | 주기적 패턴(주간/월간/연간) 탐지 |

---

## 2. Current State → Target State

```
[현재] 단일 모놀리스
──────────────────────────────────────────────────
main.py --mode full
  ├─ Crawl Pipeline (크롤링)
  └─ Analysis Pipeline (8 stages, 일회성 분석)
      Stage 1-4: 전처리 → 피처 → 감성/STEEPS → 토픽
      Stage 5-6: 시계열 → 교차분석
      Stage 7:   신호분류
      Stage 8:   출력

[목표] 듀얼 워크플로우
──────────────────────────────────────────────────
Workflow A: 크롤링 + 기본 분석 (매일 자동 실행)
  → 데이터 수집 + 일별 기본 NLP 처리
  → 환경스캐닝/시나리오 워크플로우에 데이터 공급

Workflow B: 빅데이터 통찰 분석 (축적 데이터 대상)
  → 교차언어 비교, 엔티티 궤적, 지정학 지수 등
  → 수일~수개월 축적 데이터를 분석하여 구조적 통찰 생산
```

---

## 3. Core Design Principles

### 3.1 분리 기준: "일별 처리" vs "축적 분석"

| 구분 | Workflow A (Daily) | Workflow B (Insight) |
|------|-------------------|---------------------|
| **성격** | ETL + 일별 NLP | 빅데이터 구조 분석 |
| **실행 빈도** | 매일 1회 | 주 1회 또는 수시 |
| **입력** | 당일 크롤링 JSONL | **축적된** Parquet (7일/30일/90일) |
| **시간 범위** | 단일 날짜 | 다중 날짜 윈도우 |
| **산출물** | 일별 Parquet/SQLite | **통찰 보고서** + 지수 시계열 |

### 3.2 공유 계층 분리

```
              ┌──────────────────────────────┐
              │     data/raw/{date}/          │ ← 크롤링 출력 (공유 SOT)
              │     data/processed/{date}/    │ ← Stage 1 출력 (공유)
              │     data/features/{date}/     │ ← Stage 2 출력 (공유)
              │     data/analysis/{date}/     │ ← Stage 3-4 출력 (공유)
              └──────────┬───────────────────┘
                         │ 여기까지 Workflow A가 생산
                         │
         ┌───────────────┼────────────────┐
         │               │                │
    ┌────▼────┐    ┌─────▼─────┐   ┌──────▼──────┐
    │ 현행     │    │ Workflow B │   │ 환경스캐닝   │
    │ Stage 5-8│    │ (신규)     │   │ 워크플로우   │
    │ 시계열   │    │ 빅데이터   │   │ (별도 존재)  │
    │ +신호    │    │ 통찰 분석  │   │             │
    └─────────┘    └───────────┘   └─────────────┘
```

### 3.3 실행 순서 제약

```
Workflow A (Stage 1~4) ──must complete──→ Workflow B (M1~M7)
       │                                        │
       └── Stage 5~8 (독립 계속) ────────────────┘ (병렬 가능)
```

Workflow A의 Stage 1~4가 완료되면, A의 Stage 5~8과 B의 M1~M7은 병렬 실행 가능.

---

## 4. Workflow A: Daily Pipeline (크롤링 + 기본 분석)

### 4.1 구성

```
main.py --mode daily --date 2026-04-05

Stage 1: Preprocessing    (현행 유지)  → articles.parquet
Stage 2: Feature Extract  (현행 유지)  → embeddings, tfidf, ner .parquet
Stage 3: Article Analysis (현행 유지)  → article_analysis.parquet
Stage 4: Aggregation      (현행 유지)  → topics, networks .parquet
Stage 5: Daily Output     (신규 경량)  → daily_summary.parquet + index.sqlite
```

### 4.2 변경점

기존 Stage 5(시계열), 6(교차분석), 7(신호)은 다중 날짜 데이터가 있어야 의미가 있으므로 → **Workflow B로 이관**. Stage 8의 Parquet 병합 + SQLite 인덱싱만 Stage 5(Daily Output)로 경량화.

### 4.3 산출물

```
data/
├── raw/{date}/all_articles.jsonl          ← 크롤링 원본
├── processed/{date}/articles.parquet      ← 전처리 완료
├── features/{date}/                       ← 임베딩, NER, TF-IDF
│   ├── embeddings.parquet
│   ├── tfidf.parquet
│   └── ner.parquet
├── analysis/{date}/                       ← 감성, 토픽, 네트워크
│   ├── article_analysis.parquet
│   ├── topics.parquet
│   └── networks.parquet
└── output/{date}/                         ← 일별 최종 출력
    ├── daily_summary.parquet
    └── index.sqlite
```

---

## 5. Workflow B: Insight Pipeline (빅데이터 통찰 분석)

### 5.1 Architecture Overview

```
main.py --mode insight --window 30 --end-date 2026-04-05

┌─────────────────────────────────────────────────────────┐
│  Phase 0: Window Assembly (윈도우 조립)                   │
│  N일치 daily parquet를 하나의 분석 데이터셋으로 병합        │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│  6대 분석 모듈 (M1~M6 병렬 가능)                          │
│                                                         │
│  M1: Cross-Lingual Analytics (교차언어 비교)              │
│  M2: Narrative & Framing Intelligence (서사·프레이밍)      │
│  M3: Entity Deep Analytics (엔티티 심층)                  │
│  M4: Temporal Pattern Mining (시간 구조 패턴)              │
│  M5: Geopolitical Analytics (지정학 지수)                 │
│  M6: Economic & Industry Intelligence (경제·산업)         │
│                                                         │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│  M7: Synthesis (통합 통찰 보고서)                          │
│  M1~M6 전체 산출물 → 종합 보고서 + 핵심 지표               │
└─────────────────────────────────────────────────────────┘
```

### 5.2 Module 1: Cross-Lingual Comparative Analytics

> "같은 세상을 13개 언어 공동체가 얼마나 다르게 보고 있는가"

| 지표 ID | 지표명 | 방법 | 그래뉼래리티 |
|---------|-------|------|------------|
| CL-1 | Information Asymmetry Index | 언어쌍별 토픽 분포 JSD | 일별 × 언어쌍 |
| CL-2 | Attention Gap Matrix | 언어별 토픽 주목도 격차 | 일별 × 토픽 × 언어 |
| CL-3 | Sentiment Polarity Divergence | 정렬 토픽별 감성 Wasserstein 거리 | 일별 × 토픽 × 언어쌍 |
| CL-4 | Filter Bubble Index | 언어 커뮤니티별 토픽 Jaccard 중첩 | 주별 × 언어쌍 |

**입력**: articles + topics + article_analysis (N일분)
**출력**: `data/insights/{run_id}/crosslingual/`
**최소 데이터**: 7일 × 100건/언어
**메모리**: ~1.5 GB
**핵심 라이브러리**: scipy.spatial (JSD), scipy.stats (Wasserstein)
**기존 기반**: T43 (Cross-Lingual Topic Alignment)

### 5.3 Module 2: Narrative & Framing Intelligence

> 세상이 어떻게 서술되고 있는가의 구조적 분석

| 지표 ID | 지표명 | 방법 |
|---------|-------|------|
| NF-1 | Frame Distribution Timeline | 토픽별 프레임(경제/안보/인권/과학/도덕/갈등) 분포 시계열 |
| NF-2 | Frame Shift Detection | 프레임 분포의 KL 발산 변화점 탐지 (PELT) |
| NF-3 | Voice Dominance (HHI) | 토픽별 엔티티 언급 Herfindahl-Hirschman 지수 |
| NF-4 | Media Health Score | 출처 다양성 + 프레임 다양성 + 목소리 다양성 복합 |
| NF-5 | Information Flow Topology | 소스 간 SBERT 유사도 + 발행 시간 → 방향 그래프 → PageRank |
| NF-6 | Source Credibility Score | 주장별 다수 소스 교차 확인 비율 |

**입력**: articles + NER + topics + embeddings (N일분)
**출력**: `data/insights/{run_id}/narrative/`
**최소 데이터**: 14일
**메모리**: ~2.0 GB
**핵심 라이브러리**: networkx, scipy (KL divergence, PELT)
**기존 기반**: T44 (Frame Analysis), T09 (NER)

### 5.4 Module 3: Entity Deep Analytics

> 사람, 기관, 기술, 개념의 부상과 쇠퇴를 정량적으로 추적

| 지표 ID | 지표명 | 방법 |
|---------|-------|------|
| EA-1 | Entity Trajectory Classification | 주간 PageRank 시계열 → Rising/Fading/Cyclical/Burst/Plateau |
| EA-2 | Hidden Connection Discovery | 공기어 그래프의 구조적 등가성 (Jaccard on neighbor sets) |
| EA-3 | Entity Emergence Index | 최초 출현 후 14일간 언급 가속도 |
| EA-4 | Cross-Language Entity Reach | 엔티티가 출현하는 언어 수 시계열 |

**입력**: NER + networks + articles (N일분)
**출력**: `data/insights/{run_id}/entity/`
**최소 데이터**: 30일
**메모리**: ~1.5 GB
**핵심 라이브러리**: networkx, sklearn
**기존 기반**: T41 (Centrality), T42 (Network Evolution)

### 5.5 Module 4: Temporal Pattern Mining

> 뉴스의 시간적 구조 자체를 분석

| 지표 ID | 지표명 | 방법 |
|---------|-------|------|
| TP-1 | Event Cascade Map | Multivariate Hawkes process (토픽 버스트 간 촉발 관계) |
| TP-2 | Information Velocity Matrix | 언어별 토픽 최초 출현 시차(lag) 행렬 |
| TP-3 | Attention Decay Classification | 토픽별 감쇠 곡선 피팅 (지수/멱법칙/주기적) |
| TP-4 | Structural Cyclicality | FFT 주기도분석으로 주간/월간/연간 주기 탐지 |

**입력**: articles + topics + sentiment (N일분) + Stage B1 출력
**출력**: `data/insights/{run_id}/temporal/`
**최소 데이터**: 30일 (감쇠 곡선), 90일 (주기성)
**메모리**: ~1.0 GB
**핵심 라이브러리**: tick (Hawkes), scipy.optimize (decay fitting), scipy.fft
**기존 기반**: T29 (STL), T30 (Kleinberg Burst)

### 5.6 Module 5: Geopolitical Analytics

> 뉴스 데이터에서 측정 가능한 지정학 지표 산출

| 지표 ID | 지표명 | 방법 |
|---------|-------|------|
| GI-1 | Bilateral Relations Index (BRI) | 국가쌍 동시 언급 기사의 감성 + Plutchik 감정 분해 |
| GI-2 | Soft Power Score | 국가별 보도량 + 감성 + 프레임 + 중심성 복합 |
| GI-3 | Agenda-Setting Power | 토픽별 언어 간 Granger 인과 → PageRank |
| GI-4 | Conflict-Cooperation Spectrum | 국가쌍별 분노/공포 vs 신뢰/기대 비율 |

**입력**: NER + article_analysis (N일분)
**출력**: `data/insights/{run_id}/geopolitical/`
**최소 데이터**: 14일
**메모리**: ~1.0 GB
**핵심 라이브러리**: statsmodels (Granger), networkx (PageRank)
**기존 기반**: T09 (NER), T13-14 (Sentiment), T37 (Granger)

**GDELT 대비 차별점**: GDELT는 사전 기반 감성(40개 사전). 우리 파이프라인은 트랜스포머 기반 감성 + 8차원 Plutchik 감정. "부정적"을 넘어 분노인지 공포인지 슬픔인지 구별 가능.

### 5.7 Module 6: Economic & Industry Intelligence

> 경제·산업 텍스트 인텔리전스

| 지표 ID | 지표명 | 방법 |
|---------|-------|------|
| EI-1 | Multilingual EPU Index | 언어별 "경제+정책+불확실성" 기사 비율 |
| EI-2 | Sector Sentiment Index | 산업별(에너지/기술/의료/금융/제조) 일별 감성 |
| EI-3 | Sector Sentiment Momentum | EI-2의 1차/2차 도함수 (감속=조기 경보) |
| EI-4 | Narrative Economics Tracker | 경제 서사 키워드 빈도 + 감성 × 언어별 |
| EI-5 | Technology Hype Phase | 기술 엔티티별 보도량+감성+프레임 → 하이프 단계 |

**입력**: articles + article_analysis + NER + topics (N일분)
**출력**: `data/insights/{run_id}/economic/`
**최소 데이터**: 7일 (섹터 감성), 30일 (서사 경제학)
**메모리**: ~1.5 GB
**기존 기반**: T16 (STEEPS), T08 (TF-IDF), T09 (NER)

### 5.8 Module 7: Synthesis

> 전 모듈 산출물을 통합하여 핵심 통찰 보고서 생성

**입력**: M1~M6 전체 산출물
**출력**:
- `insight_report.md` — 상위 10개 핵심 통찰 (자연어 요약)
- `insight_data.json` — 전체 지표의 구조화된 메타데이터
- `key_findings.json` — 대시보드용 집계 데이터

**Insight Report 구조**:
```markdown
# Global News Insight Brief — 2026-04-05 (30-day window)

## Cross-Lingual Asymmetry Highlights
- KO-EN JSD spiked to 0.41 (+0.18 vs baseline) — driven by [토픽]
- Attention gap: [토픽] covered 15% in DE, 0.3% in KO

## Framing Shifts
- "Nuclear energy": safety→green framing reversal across 5 languages

## Rising Entities
- [Entity]: Rising Star trajectory, cross-language reach expanded 3→8 langs

## Geopolitical Index Changes
- US-Iran BRI: -0.72 (anger-dominant), sharpest drop in 90 days
- KR agenda-setting power: +0.08 on semiconductor topics

## Economic Signals
- EPU-KO surpassed EPU-EN for first time since [date]
- "Recession narrative" propagated EN→KO with 4.2-day lag
```

---

## 6. Analysis Window Strategy

| 윈도우 | 용도 | 실행 빈도 |
|--------|------|----------|
| **7일** | 주간 브리핑 — 빠른 변화 포착 | 주 1회 |
| **30일** | 월간 심층 — 구조적 패턴 분석 | 월 1회 |
| **90일** | 분기 보고서 — 장기 궤적 + 지정학 | 분기 1회 |

---

## 7. Existing Stage 5~7 Migration

| 기존 분석 (Stage 5-7) | 새 위치 | 변경 |
|---------------------|---------|------|
| STL 분해, Prophet, ARIMA | M4 Temporal 흡수 | 다중 날짜 윈도우로 확장 |
| Granger 인과성, PCMCI | M5 Geopolitical + M1 Cross-Lingual 분산 | 토픽별 → 언어쌍별 재설계 |
| 프레임 분석, 모순 탐지 | M2 Narrative 흡수 | 시계열 추적 추가 |
| 5-Layer 신호 분류 | **환경스캐닝 워크플로우로 이관** | 이 워크플로우에서 제외 |
| 네트워크 진화 | M3 Entity 흡수 | 엔티티 궤적으로 재설계 |
| 교차언어 토픽 정렬 | M1 Cross-Lingual 흡수 | JSD/감성 분기 추가 |

---

## 8. Module Dependencies

```
Workflow A                          Workflow B
─────────                          ─────────
Stage 1 ─→ articles.parquet ───────────┐
Stage 2 ─→ embeddings.parquet ─────────┤
           tfidf.parquet ──────────────┤
           ner.parquet ────────────────┤
Stage 3 ─→ article_analysis.parquet ───┤
Stage 4 ─→ topics.parquet ────────────┤
           networks.parquet ───────────┤
                                       ▼
                              Window Assembly (Phase 0)
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
                    ▼                  ▼                   ▼
            M1 Cross-Lingual   M3 Entity         M5 Geopolitical
            M2 Narrative       M4 Temporal        M6 Economic
                    │                  │                   │
                    └──────────────────┼───────────────────┘
                                       ▼
                                  M7 Synthesis
```

**M1~M6은 상호 독립** → 병렬 실행 가능. M7만 전체에 의존.

---

## 9. Directory Structure

```
data/
├── raw/{date}/                    ← [A] 크롤링 원본
├── processed/{date}/              ← [A] Stage 1
├── features/{date}/               ← [A] Stage 2
├── analysis/{date}/               ← [A] Stage 3-4
├── output/{date}/                 ← [A] 일별 요약
│
├── insights/                      ← [B] 빅데이터 통찰 (신규)
│   ├── weekly-2026-W14/           ← 7일 윈도우 실행
│   │   ├── corpus_meta.json
│   │   ├── crosslingual/
│   │   │   ├── asymmetry_index.parquet
│   │   │   ├── attention_gaps.parquet
│   │   │   ├── sentiment_divergence.parquet
│   │   │   └── filter_bubble.parquet
│   │   ├── narrative/
│   │   │   ├── frame_evolution.parquet
│   │   │   ├── voice_dominance.parquet
│   │   │   ├── media_health.parquet
│   │   │   ├── info_flow_graph.json
│   │   │   └── source_credibility.parquet
│   │   ├── entity/
│   │   │   ├── trajectories.parquet
│   │   │   ├── rising_stars.parquet
│   │   │   └── hidden_connections.parquet
│   │   ├── temporal/
│   │   │   ├── cascades.parquet
│   │   │   ├── velocity_map.parquet
│   │   │   └── decay_curves.parquet
│   │   ├── geopolitical/
│   │   │   ├── bilateral_index.parquet
│   │   │   ├── softpower_index.parquet
│   │   │   └── agenda_influence.parquet
│   │   ├── economic/
│   │   │   ├── epu_index.parquet
│   │   │   ├── sector_sentiment.parquet
│   │   │   ├── narrative_economics.parquet
│   │   │   └── hype_cycle.parquet
│   │   └── synthesis/
│   │       ├── insight_report.md
│   │       ├── insight_data.json
│   │       └── key_findings.json
│   │
│   ├── monthly-2026-03/           ← 30일 윈도우 실행
│   │   └── (동일 구조)
│   │
│   └── quarterly-2026-Q1/         ← 90일 윈도우 실행
│       └── (동일 구조)
│
├── config/sources.yaml            ← 공유 설정
└── dedup.sqlite                   ← 공유 중복 제거 DB
```

---

## 10. CLI Interface

```bash
# Workflow A: 매일 크롤링 + 기본 분석
.venv/bin/python main.py --mode daily --date 2026-04-05

# Workflow A: 크롤링만
.venv/bin/python main.py --mode crawl --date 2026-04-05

# Workflow B: 빅데이터 통찰 분석
.venv/bin/python main.py --mode insight --window 7  --end-date 2026-04-05
.venv/bin/python main.py --mode insight --window 30 --end-date 2026-04-05
.venv/bin/python main.py --mode insight --window 90 --end-date 2026-04-05

# Workflow B: 특정 모듈만
.venv/bin/python main.py --mode insight --window 30 --module crosslingual
.venv/bin/python main.py --mode insight --window 30 --module geopolitical

# 기존 호환
.venv/bin/python main.py --mode full     # = crawl + daily (기존 동작 유지)
.venv/bin/python main.py --mode analyze  # = 기존 8-stage (하위 호환)
```

---

## 11. Source Code Structure

```
src/
├── crawling/          ← [A] 현행 유지 (116 어댑터 + 안티블록)
├── analysis/          ← [A] 현행 Stage 1~4 유지
│   ├── pipeline.py           (수정: daily/insight 모드 라우팅)
│   ├── stage1_preprocessing.py  (현행 유지)
│   ├── stage2_features.py       (현행 유지)
│   ├── stage3_article_analysis.py (현행 유지)
│   ├── stage4_aggregation.py    (현행 유지)
│   ├── stage5_daily_output.py   (신규: 기존 stage8 경량화)
│   └── legacy/                  (기존 stage5~8 보존, --mode analyze용)
│
├── insights/          ← [B] 신규 빅데이터 통찰 모듈
│   ├── __init__.py
│   ├── pipeline.py              (통찰 파이프라인 오케스트레이터)
│   ├── window_assembler.py      (Phase 0: 다중 날짜 데이터 병합)
│   ├── m1_crosslingual.py       (정보 비대칭, 주목 격차, 감성 발산)
│   ├── m2_narrative.py          (프레이밍 진화, 목소리 지배력, 정보 흐름)
│   ├── m3_entity.py             (엔티티 궤적, 숨겨진 연결)
│   ├── m4_temporal.py           (캐스케이드, 전파 속도, 감쇠 곡선)
│   ├── m5_geopolitical.py       (양자관계, 소프트파워, 의제 설정)
│   ├── m6_economic.py           (EPU, 섹터 감성, 서사 경제학, 하이프)
│   └── m7_synthesis.py          (통합 보고서 생성)
│
├── config/
│   ├── constants.py         ← INSIGHTS_DIR 등 경로 추가
│   └── insight_config.py    ← 신규: 통찰 모듈별 설정
├── storage/
└── utils/
```

---

## 12. Execution Flow

### Workflow A (Daily) — 매일 자동

```
06:00 KST  cron 트리거
    │
    ▼
  Crawl (5.5시간)
    │  112개 사이트 크롤링
    │  → data/raw/2026-04-05/all_articles.jsonl
    ▼
  Stage 1-4 (1.5시간)
    │  전처리 → 피처 → 감성/STEEPS → 토픽
    │  → data/{processed,features,analysis}/2026-04-05/
    ▼
  Daily Output (10분)
    │  → data/output/2026-04-05/daily_summary.parquet + index.sqlite
    ▼
  완료 (총 ~7시간)
```

### Workflow B (Insight) — 주간/월간/수시

```
사용자 트리거 또는 cron (일요일 18:00 KST)
    │
    ▼
  Phase 0: Window Assembly (5분)
    │  최근 N일 daily parquet 병합
    ▼
  M1~M6 병렬 실행 (30분~2시간)
    │  M1: Cross-Lingual    (JSD, 격차, 감성 발산)
    │  M2: Narrative         (프레이밍, HHI, 정보 흐름)
    │  M3: Entity            (궤적, 숨겨진 연결)
    │  M4: Temporal           (캐스케이드, 속도, 감쇠)
    │  M5: Geopolitical      (BRI, 소프트파워)
    │  M6: Economic          (EPU, 섹터 감성, 하이프)
    ▼
  M7: Synthesis (10분)
    │  → insight_report.md + insight_data.json
    ▼
  완료
```

---

## 13. Technical Dependencies

| 모듈 | 기존 코드 재활용 | 새로 구현 | 핵심 라이브러리 |
|------|----------------|---------|--------------|
| Window Assembler | — | 다중 날짜 Parquet 병합 | pandas, pyarrow |
| M1 Cross-Lingual | T43 (topic alignment) | JSD, Wasserstein, Jaccard | scipy.spatial, scipy.stats |
| M2 Narrative | T44 (frame), T09 (NER) | HHI, Shannon entropy, 정보 흐름 그래프 | networkx |
| M3 Entity | T41 (centrality), T42 (evolution) | 궤적 분류, 구조적 등가성 | networkx, sklearn |
| M4 Temporal | T29 (STL), T30 (burst) | Hawkes process, 감쇠 피팅 | tick (hawkes), scipy.optimize |
| M5 Geopolitical | T09 (NER), T13-14 (sentiment) | 국가쌍 집계, Granger on language pairs | statsmodels |
| M6 Economic | T16 (STEEPS), T08 (TF-IDF) | EPU 계산, 섹터 분류, 하이프 위상 | — |
| M7 Synthesis | — | LLM 기반 해석 (선택) | anthropic SDK |

---

## 14. Metrics Summary

**총 27개 지표**:

| 모듈 | 지표 수 | 지표 ID |
|------|--------|---------|
| M1 Cross-Lingual | 4 | CL-1 ~ CL-4 |
| M2 Narrative | 6 | NF-1 ~ NF-6 |
| M3 Entity | 4 | EA-1 ~ EA-4 |
| M4 Temporal | 4 | TP-1 ~ TP-4 |
| M5 Geopolitical | 4 | GI-1 ~ GI-4 |
| M6 Economic | 5 | EI-1 ~ EI-5 |
| **합계** | **27** | |

---

## 15. Implementation Phases

```
Phase 1 (기반): Workflow A 분리 + Window Assembler
  - main.py에 --mode daily / --mode insight 추가
  - stage5_daily_output.py 신규 작성
  - window_assembler.py 구현
  - constants.py에 인사이트 경로 추가

Phase 2 (즉시 가치): M1 Cross-Lingual + M5 Geopolitical
  - 가장 높은 고유 가치 — 13개 언어 자산 활용
  - 기존 T43, T09, T13-14 코드 대부분 재활용

Phase 3 (구조 분석): M2 Narrative + M3 Entity
  - 기존 T44, T41, T42 확장
  - 정보 흐름 위상 구조는 신규

Phase 4 (시간 분석): M4 Temporal + M6 Economic
  - 기존 T29, T30 확장 + Hawkes 신규
  - EPU/섹터 감성은 기존 STEEPS 확장

Phase 5 (통합): M7 Synthesis
  - LLM 기반 통찰 보고서 생성
  - 전체 파이프라인 통합 테스트
```

---

## 16. Research References

### Cross-Lingual Analysis
- WikiGap (2025, arXiv:2505.24195) — 언어 간 정보 비대칭 실증
- JASIST (2022) — Wikipedia 언어판 50~70% 정보 격차

### Narrative & Framing
- Scientific Reports (2025) — "Rethinking news framing with LLMs"
- ACL 2024 — "Narratives at Conflict: Multilingual Disinformation Framing"
- SemEval 2025 Task 10 — Multilingual Narrative Extraction

### Geopolitical Analytics
- BBVA Research Big Data Geopolitics Monitor — GDELT 기반 실시간 양자관계 지수
- Saadaoui (2025) — GDELT로 미-중 관계 1980-2025 복원
- Amundi — "Geopolitical risk and asset pricing across market regimes"

### Economic Intelligence
- BIS (2024) — LLM 기반 뉴스 감성이 거시경제 예측 개선
- Fed (2024) — 텍스트 분석이 산업 생산 예측 개선
- Baker-Bloom-Davis — Economic Policy Uncertainty Index

### Temporal Patterns
- Annual Review of Statistics (2025) — Hawkes Models and Applications
- Stanford WSDM (2011) — Temporal shapes of online content popularity

### Entity Analytics
- GenTKG (NAACL 2024) — Temporal Knowledge Graph Forecasting

### Media Structure
- EPJ Data Science (2024) — Information diffusion credibility inference
- EU Media Pluralism Monitor 2024 — 200 variables, 20 indicators
- Springer JCSS (2025) — Systematic review of echo chambers (129 studies)

---

## APPENDIX A: Critical Reflection #1 — 구조적 성찰 (2026-04-06)

### 치명적 결함 3개 (수정 완료)

1. **Stage 5~8 해체 시도** → 수정: 기존 8-stage 파이프라인 1바이트도 수정하지 않음. `src/insights/`를 순수 추가하여 기존 산출물을 읽기 전용 소비.
2. **SOT 설계 누락** → 수정: 런타임 SOT를 `data/insights/insight_state.json`으로 분리. 빌드 SOT(`.claude/state.yaml`)와 계층 분리.
3. **에이전트 실행 모델 미정의** → 수정: 런타임은 Python 순차 실행, 빌��는 sub-agent 순차(품질 > 속도).

### 중대 누락 5개 (수정 완료)

1. RLM 통합 → 실행 메타데이터를 KA 호환 형식으로 기록
2. Stage 5~7 중복 → 기존 유지 + B가 기존 산출물도 소비 (누적적 확장)
3. Window Assembler 메모리 → lazy loading + 컬럼 선택적 읽기 + 모듈 간 gc
4. 테스트 전략 → 모듈별 unit test + 7일 윈도우 integration test
5. config 위치 → `data/config/insights.yaml` (기존 패턴 준수)

### 수정된 핵심 원칙

- 기존 코드 수정: **0건** (순수 추가만)
- 변경 범위: `src/insights/` 신규 + `constants.py` 5줄 append + `main.py` 30줄 분기 추가
- 롤백: `rm -rf src/insights/`로 완료

---

## APPENDIX B: Critical Reflection #2 — 할루시네이션 봉쇄 성찰 (2026-04-06)

### 핵심 원칙

27개 지표를 3개 Type으로 전수 분류하여, 산술/통계 연산(Type A)과 규칙 기반 분류(Type B)는 Python 코드로 원천봉쇄하고, 의미론적 해석(Type C)만 LLM에 허용한다.

### 전수 분류 결과

| Type | 지표 수 | 비율 | 실행자 |
|------|--------|------|--------|
| **A: 순수 산술** | 19개 | 70% | Python 코드 (결정론적, 검증 가능) |
| **B: 규칙 기반 분류** | 6개 | 22% | Python 코드 (임계값/키워드 기반, 결정론적) |
| **C: 의미론적 해석** | 2개 | 8% | LLM (M7 Synthesis만 — 입출력 P1 검증) |

#### Type A 지표 (19개 — Python 순수 산술)

- CL-1 정보 비대칭 (JSD), CL-2 주목 격차, CL-3 감성 발산 (Wasserstein), CL-4 필터 버블 (Jaccard)
- NF-2 프레임 전환 (PELT), NF-3 목소리 지배력 (HHI), NF-4 미디어 건강 (Shannon), NF-5 정보 흐름 (PageRank), NF-6 출처 신뢰도
- EA-2 숨겨진 연결 (Jaccard), EA-3 부상 지수, EA-4 교차언어 도달
- TP-1 캐스케이드 (Hawkes), TP-2 전파 속도, TP-4 주기성 (FFT)
- GI-1 양자관계 (BRI), GI-3 의제 설정 (Granger), GI-4 갈등-협력
- EI-3 감성 모멘텀

#### Type B 지표 (6개 — Python 규칙 기반)

- NF-1 프레임 분포 → 기존 DistilBART zero-shot (T16 패턴 재활용)
- EA-1 궤적 분류 → slope + std 기반 규칙 (임계값 상수)
- TP-3 감쇠 곡선 → R² 비교로 자동 선택 (지수 vs 멱법칙)
- GI-2 소프트파워 → 4개 구성요소 가중 합 (가중치 상수)
- EI-2 섹터 분류 → 다국어 키워드 사전 매칭
- EI-5 하이프 위상 → volume_trend + sentiment 기반 규칙

#### Type C 지표 (2개 — M7 Synthesis만)

- M7 패턴 해석 → LLM. 단, 입력은 P1 검증된 JSON 요약만
- M7 보고서 생성 → LLM. 단, 출력은 P1 수치 정합성 검증

### M7 할루시네이션 관리 구조

```
P1 Python (Pre): M1~M6 정량 결과에서 구조화된 JSON 요약 생성
    ↓ (검증된 숫자만 전달)
LLM (Interpretation): 숫자를 해석, 함의 도출, 자연어 보고서
    ↓
P1 Python (Post): 보고서 내 인용 수치 ↔ 원본 JSON 교차 검증
```

### P1 검증 함수 설계

`validate_insight_metrics()` — 27개 지표 전수 범위/산술 검증:
- CL-1: JSD ∈ [0, 1], 대칭성
- NF-3: HHI ∈ [0, 1]
- GI-1: BRI ∈ [-1, 1]
- EI-1: EPU ∈ [0, 1]
- 각 지표별 수학적 범위 제약 전수 검증

### Type B 분류의 결정론성 보장

궤적 분류, 감쇠 곡선 분류, 하이프 위상 분류 등 규칙 기반 분류의 **임계값을 `data/config/insights.yaml`로 외부화**하여:
1. 코드 내 매직넘버 방지
2. 임계값 변경 시 코드 수정 불필요
3. 동일 입력 + 동일 설정 = 동일 출력 (완전 결정론적)

### 섹터 분류 키워드 사전

`data/config/insights.yaml`에 5개 섹터 × 13개 언어 키워드 사전을 정의. zero-shot 모델 대신 키워드 매칭으로 100% 결정론적 + 재현 가능한 분류. 품질 향상은 키워드 사전 확장으로 달성 (코드 수정 불필요).

---

## APPENDIX C: Critical Reflection #3 — 설계 결함 최종 재조사 (2026-04-06)

### Gap 1: 빌드 워크플로우 — 기존 워크플로우 완료 상태에서 새 단계 추가

**문제**: state.yaml `status: complete`이므로 SM-ST1 완료 보호에 의해 새 step 추가가 물리적으로 차단됨.
**해결**: status를 `in_progress`로 되돌리고 total_steps를 20→25로 업데이트. 기존 워크플로우에 Step 21~25 추가 (기능 개선으로서).
**리스크**: `/start` 라우팅이 status!=complete를 감지하여 `/start`로 라우팅 — workflow.md에 Step 21~25 정의 필수.

### Gap 2: 데이터 가용성 전제 조건 미검증

**문제**: 윈도우 내 날짜 중 데이터 부재/불완전한 날짜가 있을 수 있음.
**해결**: `validate_window_availability()` P1 함수로 최소 70% 커버리지 + 모듈별 최소 요건 검증. 미충족 모듈은 skip, 나머지 실행.

### Gap 3: NF-1 프레임 분류와 STEEPS 중복

**문제**: NF-1의 프레임 분류 체계가 기존 STEEPS와 다름 → 54,000건 재추론 필요.
**해결**: 새 분류 체계 도입하지 않음. **기존 STEEPS를 프레임으로 직접 재활용**. NF-1은 STEEPS 분포의 시계열 분석만 수행 (Type A 산술). 재추론 불필요.

### Gap 4: `tick` 라이브러리 Python 3.13 비호환

**문제**: M4 TP-1 Hawkes process의 `tick` 라이브러리가 Python 3.13에서 컴파일 실패 가능.
**해결**: tick 의존성 제거. scipy.optimize 기반 자체 MLE 추정(단변량) + 기존 T37 Granger 인과성 재활용(다변량 대체).

### Gap 5: Stage 5~7 산출물 소비 불명확

**문제**: Stage 5~7은 단일 날짜 기준이므로 다중 날짜 윈도우에 직접 사용 불가.
**해결**: **Workflow B는 Stage 1~4 산출물만 소비.** Stage 5~7에 무의존. Stage 5~7의 기능(시계열, 교차분석)은 Workflow B가 다중 날짜 기준으로 자체 수행.

### Gap 6: M7 LLM 호출 — C1 제약 "Claude API = $0" 충돌

**문제**: M7에서 LLM 기반 해석/보고서를 하면 C1 위반.
**해결**: 듀얼 모드 — Template Mode(기본, 완전 결정론적, C1 준수) + LLM Mode(선택적, `--llm-synthesis` 플래그, 사용자 명시적 요청 시만). **기본값은 Template Mode.**

### Gap 7: insights.yaml 과잉 설계

**문제**: 초기부터 복잡한 YAML은 과잉.
**해결**: 최소 YAML(모듈 enabled/disabled + 기본 윈도우만) + 임계값은 `src/insights/constants.py`에 Python 상수 (기존 constants.py 패턴 준수).

### Reflection #3 이후 수정된 지표 분류

NF-1이 Type B(모델 의존)에서 **Type A(산술)로 변경** — STEEPS 직접 재활용으로 인해:

| Type | 지표 수 | 비율 |
|------|--------|------|
| A: 순수 산술 | **20개** | **74%** (19→20, NF-1 이동) |
| B: 규칙 기반 분류 | **5개** | **19%** (6→5, NF-1 이동) |
| C: 의미론적 해석 | **2개** | 7% (변경 없음, 단 기본값은 Template Mode) |

### 최종 의존성 매트릭스

```
Workflow B 입력 (Stage 1~4 산출물만):
  articles.parquet        ← Stage 1 (전처리)
  embeddings.parquet      ← Stage 2 (SBERT)
  ner.parquet             ← Stage 2 (NER)
  article_analysis.parquet ← Stage 3 (감성, STEEPS)
  topics.parquet          ← Stage 4 (BERTopic)
  networks.parquet        ← Stage 4 (공기어)

Workflow B가 소비하지 않는 것:
  timeseries.parquet      ← Stage 5 (단일 날짜 — B가 자체 수행)
  cross_analysis.parquet  ← Stage 6 (단일 날짜 — B가 자체 수행)
  signals.parquet         ← Stage 7 (환경스캐닝 영역)
```
