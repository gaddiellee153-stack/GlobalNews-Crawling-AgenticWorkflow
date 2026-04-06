Execute the GlobalNews crawling and analysis pipeline to collect real news data and perform big data analysis.

## Instructions

This command runs the **actual system** (not the workflow builder). It crawls 116 international news sites (Groups A-J), processes articles through an 8-stage NLP pipeline, and produces Parquet/SQLite output.

### Step 0: Ensure Domain Venv

The NLP pipeline requires Python 3.13 venv (spaCy is incompatible with Python 3.14 due to pydantic v1). All domain commands below use `.venv/bin/python` directly — **do NOT rely on `source .venv/bin/activate`** (Claude Code Bash tool does not persist shell state between calls).

If `.venv` does not exist, create it:
```bash
/opt/homebrew/bin/python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt && .venv/bin/python -m spacy download en_core_web_sm
```

Verify the venv is healthy:
```bash
.venv/bin/python -c "import spacy; nlp = spacy.load('en_core_web_sm'); print(f'spaCy {spacy.__version__}, {len(nlp.pipe_names)} pipes')"
```

> **Note**: Hook scripts (`.claude/hooks/scripts/`) run on system python3 and are unaffected. Only domain code (`main.py`, `src/`) requires the venv.

### Step 1: Pre-flight Check

Run the pre-flight validation to ensure the environment is ready:

```bash
.venv/bin/python scripts/preflight_check.py --project-dir . --mode full --json
```

Parse the JSON output:
- **`readiness: "ready"`**: Proceed to Step 2.
- **`readiness: "blocked"`**: Report `critical_failures` to user. Suggest fixes.
- **`degradations`**: Report any capability limitations (e.g., patchright missing) but proceed.

Display a summary to the user:
```
── PIPELINE EXECUTION ─────────────────────────
Mode: full (crawl + analyze)
Sites: {enabled_sites}/{total_sites} enabled
Date: {today}
Degradations: {list or "none"}
───────────────────────────────────────────────
```

### Step 2: Dry Run Validation

Before actual execution, run a dry-run to validate configuration:

```bash
.venv/bin/python main.py --mode full --dry-run 2>&1
```

If the dry-run fails, report the error and stop.

### Step 3: Execute the Pipeline

Based on user request, run the appropriate mode:

#### Full Pipeline (crawl + analyze)
```bash
.venv/bin/python main.py --mode full --date $(date +%Y-%m-%d) --log-level INFO 2>&1
```

#### Crawl Only
```bash
.venv/bin/python main.py --mode crawl --date $(date +%Y-%m-%d) --log-level INFO 2>&1
```

#### Analyze Only (requires prior crawl data)
```bash
.venv/bin/python main.py --mode analyze --all-stages --log-level INFO 2>&1
```

#### Specific Sites/Groups
```bash
.venv/bin/python main.py --mode crawl --date $(date +%Y-%m-%d) --groups A,B --log-level INFO 2>&1
```

#### Insight Analysis (Big Data Insights on accumulated data)
```bash
# 30-day window (default)
.venv/bin/python main.py --mode insight --window 30 --end-date $(date +%Y-%m-%d) --log-level INFO 2>&1

# Weekly window
.venv/bin/python main.py --mode insight --window 7 --end-date $(date +%Y-%m-%d) --log-level INFO 2>&1

# Specific module only
.venv/bin/python main.py --mode insight --window 30 --module m1_crosslingual --log-level INFO 2>&1
```

After insight analysis completes, show the report:
```bash
cat data/insights/$(ls -t data/insights/ | head -1)/synthesis/insight_report.md
```

**Important**: Run in foreground (not background) so progress can be monitored and errors reported in real-time.

### Step 4: Monitor and Report

During execution, monitor the output for:
- Per-site crawl results (articles collected, failures)
- Analysis stage progress (Stage 1 through 8)
- Memory usage warnings
- Error patterns

After completion, show results:
```bash
.venv/bin/python main.py --mode status 2>&1
```

Report:
1. **Crawling summary**: articles collected per site/group, failures
2. **Analysis summary**: stages completed, processing time, memory peak
3. **Output files**: list generated Parquet/SQLite files with sizes
4. **Errors**: any sites that failed and why

### Step 5: Data Inventory

Show the user what was produced:
```bash
find data/raw -type f -name "*.jsonl" | head -5
find data/output -type f | head -10
ls -lh data/output/ data/processed/ data/analysis/ 2>/dev/null
```

## Mode Selection

If the user's request implies a specific mode:

| User says | Mode |
|-----------|------|
| "크롤링을 하자", "뉴스를 수집하자", "기사를 가져와" | `crawl` |
| "분석을 하자", "빅데이터 분석", "NLP 파이프라인" | `analyze` |
| "시작하자", "전체 실행", "풀 파이프라인" | `full` |
| "한국 뉴스만", "Group A만" | `crawl --groups A,B` |
| "통찰 분석", "인사이트 분석", "빅데이터 통찰" | `insight --window 30` |
| "주간 통찰", "이번 주 통찰" | `insight --window 7` |
| "분기 통찰", "3개월 통찰" | `insight --window 90` |
| "상태 확인", "결과 확인" | `status` |

## Error Handling

- **spaCy broken (Python 3.14)**: main.py has a runtime version guard that refuses to run on Python 3.14+. Always use `.venv/bin/python main.py` (not `python3 main.py`). If venv is missing, see Step 0.
- **patchright missing**: Sites requiring headless browser (Extreme difficulty) will be skipped. Most sites use RSS/sitemap and are unaffected.
- **Network errors**: Individual site failures are logged and skipped; pipeline continues to next site.
- **Memory limit exceeded**: Pipeline auto-aborts at 10 GB. Suggest running smaller site groups.
- **Timeout**: Default 4-hour timeout via run_daily.sh. Direct execution via main.py has no timeout.

## Quick Test (Small Scale)

For a quick test with minimal sites:
```bash
# Test with 2-3 Korean sites only (Group A, fastest)
.venv/bin/python main.py --mode crawl --date $(date +%Y-%m-%d) --sites chosun,yna --log-level DEBUG 2>&1
```
