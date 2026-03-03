# Orchestrator Playbook — GlobalNews Auto-Build

> **Purpose**: Step-by-step execution guide for the Orchestrator (LLM). Each step specifies exactly which P1 scripts to call, which agents to spawn, and how to handle failures. The LLM does creative work; Python enforces state management.

## Quick Reference

| Script | Role | When |
|--------|------|------|
| `scripts/sot_manager.py` | Atomic SOT read/write | Every step start/end |
| `scripts/validate_step_transition.py` | Pre-conditions for step advance | Before `--advance-step` |
| `scripts/run_quality_gates.py` | L0→L1→L1.5→L2 sequencing | After output saved |
| `scripts/validate_site_coverage.py` | 44-site completeness | Steps 1, 6, 11 |
| `scripts/validate_technique_coverage.py` | 56-technique completeness | Step 7 |
| `scripts/validate_code_structure.py` | Code structure compliance | Steps 9-15 |
| `scripts/validate_data_schema.py` | Parquet/config schema | Steps 5, 9 |
| `scripts/validate_team_state.py` | Team lifecycle consistency | Steps 2, 6, 10, 11, 13, 14 |
| `scripts/extract_orchestrator_step_guide.py` | Extract step-specific guide from this playbook | Context injection for agents |

### SOT Command Cheat Sheet

```bash
# Read current state
python3 scripts/sot_manager.py --read --project-dir .

# Record output for step N
python3 scripts/sot_manager.py --record-output N path/to/output.md --project-dir .

# Advance to next step
python3 scripts/sot_manager.py --advance-step N --project-dir .

# Update pACS scores
python3 scripts/sot_manager.py --update-pacs N --F 85 --C 78 --L 80 --project-dir .

# Update team state
python3 scripts/sot_manager.py --update-team '{"name":"team-x","status":"partial","tasks_completed":[],"tasks_pending":["t1","t2"]}' --project-dir .

# Set workflow status (e.g., mark completed after Step 20)
python3 scripts/sot_manager.py --set-status completed --project-dir .

# Autopilot: enable/disable
python3 scripts/sot_manager.py --set-autopilot true --project-dir .

# Autopilot: record auto-approved human step (must be 4/8/18)
python3 scripts/sot_manager.py --add-auto-approved 8 --project-dir .

# Autopilot: P1 decision log validation (DL1-DL6, after creating decision log)
python3 .claude/hooks/scripts/validate_decision_log.py --step 8 --project-dir .

# Quality gates — auto-detects autopilot from SOT, runs HQ1/HQ2/HQ3 if enabled
python3 scripts/run_quality_gates.py --step 8 --project-dir .

# Extract step-specific guide (focused context for agents — avoids loading full playbook)
python3 scripts/extract_orchestrator_step_guide.py --step 12 --project-dir .
python3 scripts/extract_orchestrator_step_guide.py --step 12 --project-dir . --include-universal --include-failure-recovery
```

### Universal Step Protocol

Every step (unless stated otherwise) follows this sequence:

```
1. READ SOT → confirm current_step == N
2. Read Verification criteria from prompt/workflow.md Step N
   (the "Verification:" field defines "100% complete" for this step)
3. Run Pre-processing scripts (if any)
4. Spawn Agent / Create Team
5. Agent produces output → save to disk
5a. [Steps 9-17 only] Pre-L1 /simplify Quality Pass → apply suggestions (non-blocking)
6. Run Post-processing scripts (if any)
7. Record output: sot_manager.py --record-output N <path>
8. Run domain-specific validators
9. Quality Gates: L0 → L1 (against Verification criteria) → L1.5 → (L2 if Review step)
   L2 Review: Read config/review-focus.yaml Step N → inject focus context into @reviewer/@fact-checker prompt
10. Translation (if @translator step)
11. Validate transition: validate_step_transition.py --step N
12. Advance: sot_manager.py --advance-step N
```

> **Verification Criteria Source**: Each step's Verification criteria are defined in
> `prompt/workflow.md` under the corresponding Step N section. The Orchestrator MUST
> read `prompt/workflow.md` to know what "100% complete" means for each step.

#### Agent Spawn Protocol (Step 4 — examples)

**Solo agent** (e.g., Step 1 `@site-recon`):
```
Task agent with: Read prompt/workflow.md Step 1 + previous outputs → produce output
```

**(team) step** (e.g., Step 2 research team):
```bash
# Create team
TeamCreate --name "research-team" --members "@feasibility-analyst, @legal-analyst"

# Assign tasks via SendMessage
SendMessage --to "@feasibility-analyst" --message "Read research/site-reconnaissance.md → produce crawling feasibility analysis"

# Monitor and collect (Team Lead waits for reports)
# Update SOT after each teammate completes
python3 scripts/sot_manager.py --update-team '{"name":"research-team","status":"partial","tasks_completed":[],"tasks_pending":["feasibility-analyst","legal-analyst"],"completed_summaries":{}}'
```

---

## Research Phase (Steps 1–4)

### Step 1: Target Site Reconnaissance & Classification

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@site-recon` (sonnet) |
| **Review** | `@fact-checker` |
| **Translation** | `@translator` |
| **Output** | `research/site-reconnaissance.md` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .
# → current_step must be 1

# 2. Pre-processing
python3 scripts/extract_site_urls.py --project-dir .

# 3. Spawn @site-recon
#    Task: Read extracted site list + PRD → visit all 44 sites → produce reconnaissance

# 4. Save output to research/site-reconnaissance.md

# 5. Post-processing
python3 scripts/generate_sources_yaml_draft.py --project-dir .

# 6. Record output
python3 scripts/sot_manager.py --record-output 1 research/site-reconnaissance.md --project-dir .

# 7. Domain validation
python3 scripts/validate_site_coverage.py --file research/site-reconnaissance.md --project-dir .
# → SC1-SC4: all 44 sites must be present

# 8. Quality Gates (manual — LLM performs L1 Verification + L1.5 pACS)
#    L1: Verify each Verification checkbox from workflow.md Step 1
#    L1.5: Pre-mortem → F/C/L scoring → pACS = min(F,C,L)
python3 scripts/sot_manager.py --update-pacs 1 --F <score> --C <score> --L <score> --project-dir .

# 9. L2 Review: Spawn @fact-checker
#    Save report to review-logs/step-1-review.md
python3 .claude/hooks/scripts/validate_review.py --step 1 --project-dir . --check-pacs-arithmetic

# 10. Translation: Spawn @translator
#     Save to research/site-reconnaissance.ko.md
python3 .claude/hooks/scripts/validate_translation.py --step 1 --project-dir . --check-pacs --check-sequence

# 11. Validate transition
python3 scripts/validate_step_transition.py --step 1 --project-dir .
# → ST1-ST6 all must pass

# 12. Advance
python3 scripts/sot_manager.py --advance-step 1 --project-dir .
```

**Failure Handling:**
- Verification FAIL → retry budget check → Abductive Diagnosis → re-run @site-recon on failed sites
- pACS RED (< 50) → retry budget check → diagnosis → rework
- Review FAIL → retry budget check → diagnosis → address reviewer issues → re-review

---

### Step 2: (team) Technology Stack Validation

| Item | Value |
|------|-------|
| **Type** | Team |
| **Team** | `tech-validation-team` |
| **Members** | `@dep-validator`, `@nlp-benchmarker`, `@memory-profiler` |
| **Review** | None |
| **Translation** | None |
| **Output** | `research/tech-validation.md` (merged) |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Create team
python3 scripts/sot_manager.py --update-team '{"name":"tech-validation-team","status":"partial","tasks_completed":[],"tasks_pending":["dep-validator","nlp-benchmarker","memory-profiler"],"completed_summaries":{}}' --project-dir .

# 3. Spawn team via TeamCreate
#    → @dep-validator: install+verify all PRD §8.1 packages
#    → @nlp-benchmarker: benchmark Korean NLP models
#    → @memory-profiler: profile memory scenarios

# 4. As each teammate completes, update team state:
python3 scripts/sot_manager.py --update-team '{"name":"tech-validation-team","status":"partial","tasks_completed":["dep-validator"],"tasks_pending":["nlp-benchmarker","memory-profiler"],"completed_summaries":{"dep-validator":"<summary>"}}' --project-dir .

# 5. Validate team state
python3 scripts/validate_team_state.py --project-dir .
# → TS1-TS4 must pass

# 6. Team Lead merges → research/tech-validation.md
# 7. Record output
python3 scripts/sot_manager.py --record-output 2 research/tech-validation.md --project-dir .

# 8. Quality Gates (L1 + L1.5 — no L2 review for this step)
python3 scripts/sot_manager.py --update-pacs 2 --F <score> --C <score> --L <score> --project-dir .

# 9. Mark team complete
python3 scripts/sot_manager.py --update-team '{"name":"tech-validation-team","status":"all_completed","tasks_completed":["dep-validator","nlp-benchmarker","memory-profiler"],"tasks_pending":[],"completed_summaries":{}}' --project-dir .
# NOTE: completed_summaries populated by Orchestrator from accumulated teammate reports

# 10. Validate + Advance
python3 scripts/validate_step_transition.py --step 2 --project-dir .
python3 scripts/sot_manager.py --advance-step 2 --project-dir .
```

**Failure Handling:**
- Teammate failure → SendMessage with feedback → teammate retries (max 3 per I-3)
- Package install failure → @dep-validator documents alternative + NO-GO recommendation
- Memory > 16GB → @memory-profiler recommends sequential loading strategy

---

### Step 3: Crawling Feasibility Analysis

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@crawl-analyst` (opus) |
| **Review** | `@fact-checker` |
| **Translation** | `@translator` |
| **Output** | `research/crawling-feasibility.md` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/merge_recon_and_deps.py --project-dir .

# 3. Spawn @crawl-analyst
#    Input: merged recon+deps data
#    Task: Design per-site crawling approach with 4-level retry architecture

# 4. Save output to research/crawling-feasibility.md

# 5. Record output
python3 scripts/sot_manager.py --record-output 3 research/crawling-feasibility.md --project-dir .

# 6. Domain validation
python3 scripts/validate_site_coverage.py --file research/crawling-feasibility.md --project-dir .

# 7. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 3 --F <score> --C <score> --L <score> --project-dir .

# 8. L2 Review: @fact-checker
python3 .claude/hooks/scripts/validate_review.py --step 3 --project-dir . --check-pacs-arithmetic

# 9. Translation: @translator
python3 .claude/hooks/scripts/validate_translation.py --step 3 --project-dir . --check-pacs --check-sequence

# 10. Advance
python3 scripts/validate_step_transition.py --step 3 --project-dir .
python3 scripts/sot_manager.py --advance-step 3 --project-dir .
```

---

### Step 4: (human) Research Review & Prioritization

| Item | Value |
|------|-------|
| **Type** | Human checkpoint |
| **Command** | `/review-research` |
| **Output** | Decision record |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Execute /review-research (reads Steps 1-3, runs validators)
#    OR Autopilot: auto-approve with quality-maximizing defaults

# 3. Options: proceed / rework [step] / modify

# 4. Record decision
python3 scripts/sot_manager.py --record-output 4 autopilot-logs/step-4-decision.md --project-dir .

# 5. Autopilot only: P1 decision log validation (DL1-DL6)
python3 .claude/hooks/scripts/validate_decision_log.py --step 4 --project-dir .

# 6. Autopilot only: record auto-approval
python3 scripts/sot_manager.py --add-auto-approved 4 --project-dir .

# 7. Quality gates — auto-detects autopilot from SOT, runs HQ1/HQ2/HQ3 if enabled
python3 scripts/run_quality_gates.py --step 4 --project-dir .

# 8. Validate (ST7 checks decision log exists when autopilot active) + Advance
python3 scripts/validate_step_transition.py --step 4 --project-dir .
python3 scripts/sot_manager.py --advance-step 4 --project-dir .
```

---

## Planning Phase (Steps 5–8)

### Step 5: System Architecture Blueprint

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@system-architect` (opus) |
| **Review** | `@reviewer` |
| **Translation** | `@translator` |
| **Output** | `planning/architecture-blueprint.md` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/filter_prd_architecture.py --project-dir .

# 3. Spawn @system-architect
#    Input: PRD §6-8 filtered content + Research outputs
#    Task: Design complete 4-layer architecture with Parquet/SQLite schemas

# 4. Save output to planning/architecture-blueprint.md

# 5. Record output
python3 scripts/sot_manager.py --record-output 5 planning/architecture-blueprint.md --project-dir .

# 6. Domain validation
python3 scripts/validate_data_schema.py --step 5 --project-dir .
# → DS1-DS3: all PRD §7.1 columns must be present in architecture doc

# 7. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 5 --F <score> --C <score> --L <score> --project-dir .

# 8. L2 Review: @reviewer
python3 .claude/hooks/scripts/validate_review.py --step 5 --project-dir . --check-pacs-arithmetic

# 9. Translation: @translator
python3 .claude/hooks/scripts/validate_translation.py --step 5 --project-dir . --check-pacs --check-sequence

# 10. Advance
python3 scripts/validate_step_transition.py --step 5 --project-dir .
python3 scripts/sot_manager.py --advance-step 5 --project-dir .
```

---

### Step 6: (team) Per-Site Crawling Strategy Design

| Item | Value |
|------|-------|
| **Type** | Team |
| **Team** | `crawl-strategy-team` |
| **Members** | `@crawl-strategist-kr`, `@crawl-strategist-en`, `@crawl-strategist-asia`, `@crawl-strategist-global` |
| **Output** | `planning/crawling-strategies.md` (merged) |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/split_sites_by_group.py --project-dir .

# 3. Create team
python3 scripts/sot_manager.py --update-team '{"name":"crawl-strategy-team","status":"partial","tasks_completed":[],"tasks_pending":["crawl-strategist-kr","crawl-strategist-en","crawl-strategist-asia","crawl-strategist-global"],"completed_summaries":{}}' --project-dir .

# 4. Spawn team via TeamCreate
#    4 strategists work in parallel on their site groups
#    Outputs: planning/crawl-strategy-korean.md, planning/crawl-strategy-english.md,
#             planning/crawl-strategy-asia.md, planning/crawl-strategy-global.md

# 5. Update team state as each member completes (repeat for each)
python3 scripts/validate_team_state.py --project-dir .

# 6. Team Lead merges → planning/crawling-strategies.md

# 7. Record output
python3 scripts/sot_manager.py --record-output 6 planning/crawling-strategies.md --project-dir .

# 8. Domain validation
python3 scripts/validate_site_coverage.py --file planning/crawling-strategies.md --project-dir .
# → All 44 sites must be present

# 9. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 6 --F <score> --C <score> --L <score> --project-dir .

# 10. Finalize team + Advance
python3 scripts/sot_manager.py --update-team '{"name":"crawl-strategy-team","status":"all_completed","tasks_completed":["crawl-strategist-kr","crawl-strategist-en","crawl-strategist-asia","crawl-strategist-global"],"tasks_pending":[],"completed_summaries":{}}' --project-dir .
python3 scripts/validate_step_transition.py --step 6 --project-dir .
python3 scripts/sot_manager.py --advance-step 6 --project-dir .
```

---

### Step 7: Analysis Pipeline Detailed Design

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@pipeline-designer` (opus) |
| **Review** | `@reviewer` |
| **Translation** | `@translator` |
| **Output** | `planning/analysis-pipeline-design.md` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/filter_prd_analysis.py --project-dir .

# 3. Spawn @pipeline-designer
#    Task: Design all 8 stages with input/output formats, library assignments,
#          56 techniques mapped to stages, 5-Layer classification rules

# 4. Save output to planning/analysis-pipeline-design.md

# 5. Record output
python3 scripts/sot_manager.py --record-output 7 planning/analysis-pipeline-design.md --project-dir .

# 6. Domain validation
python3 scripts/validate_technique_coverage.py --file planning/analysis-pipeline-design.md --project-dir .
# → TC1-TC4: all 56 techniques must be present and stage-mapped

# 7. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 7 --F <score> --C <score> --L <score> --project-dir .

# 8. L2 Review: @reviewer
python3 .claude/hooks/scripts/validate_review.py --step 7 --project-dir . --check-pacs-arithmetic

# 9. Translation: @translator
python3 .claude/hooks/scripts/validate_translation.py --step 7 --project-dir . --check-pacs --check-sequence

# 10. Advance
python3 scripts/validate_step_transition.py --step 7 --project-dir .
python3 scripts/sot_manager.py --advance-step 7 --project-dir .
```

---

### Step 8: (human) Architecture & Strategy Approval

| Item | Value |
|------|-------|
| **Type** | Human checkpoint |
| **Command** | `/review-architecture` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Execute /review-architecture
#    Reads Steps 5-7, runs validate_data_schema, validate_site_coverage, validate_technique_coverage

# 3. Options: proceed / rework [step] / modify

# 4. Record decision
python3 scripts/sot_manager.py --record-output 8 autopilot-logs/step-8-decision.md --project-dir .

# 5. Autopilot only: P1 decision log validation (DL1-DL6)
python3 .claude/hooks/scripts/validate_decision_log.py --step 8 --project-dir .

# 6. Autopilot only: record auto-approval
python3 scripts/sot_manager.py --add-auto-approved 8 --project-dir .

# 7. Quality gates — auto-detects autopilot from SOT, runs HQ1/HQ2/HQ3 if enabled
python3 scripts/run_quality_gates.py --step 8 --project-dir .

# 8. Validate (ST7 checks decision log exists when autopilot active) + Advance
python3 scripts/validate_step_transition.py --step 8 --project-dir .
python3 scripts/sot_manager.py --advance-step 8 --project-dir .
```

---

## Implementation Phase (Steps 9–20)

> **CAP Reminder**: All coding agents in this phase internalize Coding Anchor Points — read before modify (CAP-1), minimum code (CAP-2), define success criteria first (CAP-3), touch only what's needed (CAP-4). Details: AGENTS.md §2.

### Step 9: Project Infrastructure Scaffolding

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@infra-builder` (opus) |
| **Output** | Project infrastructure (code files) |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Spawn @infra-builder
#    Task: Create directory structure, sources.yaml, pipeline.yaml, venv,
#          requirements.txt, Python package structure, shared utilities, main.py
#    Uses: Step 5 architecture blueprint, Step 6 strategies for sources.yaml

# 3. Verify infrastructure
python3 -c "import src; print('Package structure OK')"
python3 scripts/validate_code_structure.py --step 9 --project-dir .
# → CS1-CS4: directories, files, imports, CLI all present
python3 scripts/validate_data_schema.py --step 9 --check-config --project-dir .
# → DS_CFG1-7: sources.yaml (44 sites) + pipeline.yaml (8 stages) valid

# 4. Record output (use a sentinel file for code-only steps)
#    Create a manifest file listing all created files
python3 scripts/sot_manager.py --record-output 9 src/__init__.py --project-dir .

# 5. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 9 --F <score> --C <score> --L <score> --project-dir .

# 6. Advance
python3 scripts/validate_step_transition.py --step 9 --project-dir .
python3 scripts/sot_manager.py --advance-step 9 --project-dir .
```

---

### Step 10: (team) Crawling Core Engine Implementation

| Item | Value |
|------|-------|
| **Type** | Team |
| **Team** | `crawl-engine-team` |
| **Checkpoint** | Dense (CP-1, CP-2, CP-3 per member) |
| **Members** | `@crawler-core-dev`, `@anti-block-dev`, `@dedup-dev`, `@ua-rotation-dev` |
| **Output** | `src/crawling/` (code files) |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/extract_architecture_crawling.py --project-dir .

# 3. Create team
python3 scripts/sot_manager.py --update-team '{"name":"crawl-engine-team","status":"partial","tasks_completed":[],"tasks_pending":["crawler-core-dev","anti-block-dev","dedup-dev","ua-rotation-dev"],"completed_summaries":{}}' --project-dir .

# 4. Spawn team — Dense Checkpoint Pattern:
#    Each member reports at CP-1, CP-2, CP-3
#    Team Lead reviews at each checkpoint via SendMessage

# 5. Update team state after each member completes
python3 scripts/validate_team_state.py --project-dir .

# 6. Team Lead integration test
#    Verify module interfaces are compatible

# 7. Validate code structure
python3 scripts/validate_code_structure.py --step 10 --project-dir .

# 8. Record output
python3 scripts/sot_manager.py --record-output 10 src/crawling/crawler.py --project-dir .

# 9. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 10 --F <score> --C <score> --L <score> --project-dir .

# 10. Finalize team + Advance
python3 scripts/sot_manager.py --update-team '{"name":"crawl-engine-team","status":"all_completed","tasks_completed":["crawler-core-dev","anti-block-dev","dedup-dev","ua-rotation-dev"],"tasks_pending":[],"completed_summaries":{}}' --project-dir .
python3 scripts/validate_step_transition.py --step 10 --project-dir .
python3 scripts/sot_manager.py --advance-step 10 --project-dir .
```

---

### Step 11: (team) Site-Specific Crawler Adapters (44 sites)

| Item | Value |
|------|-------|
| **Type** | Team |
| **Team** | `site-adapters-team` |
| **Checkpoint** | Dense |
| **Members** | `@adapter-dev-kr-major` (11), `@adapter-dev-kr-tech` (8), `@adapter-dev-english` (12), `@adapter-dev-multilingual` (13) |
| **Output** | `src/crawling/adapters/` (44 adapter files) |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/distribute_sites_to_teams.py --project-dir .

# 3. Create team
python3 scripts/sot_manager.py --update-team '{"name":"site-adapters-team","status":"partial","tasks_completed":[],"tasks_pending":["adapter-dev-kr-major","adapter-dev-kr-tech","adapter-dev-english","adapter-dev-multilingual"],"completed_summaries":{}}' --project-dir .

# 4. Spawn team — Dense Checkpoint Pattern
#    Each member reports at CP-1, CP-2, CP-3 with tested adapter counts

# 5. Update team state after each member completes
python3 scripts/validate_team_state.py --project-dir .

# 6. Domain validation — CRITICAL
python3 scripts/validate_code_structure.py --step 11 --check-adapters --project-dir .
# → CS5: all 44 adapters must exist
python3 scripts/verify_adapter_coverage.py --project-dir .
# → Cross-check 44 URLs in sources.yaml against adapter registry

# 7. Record output
python3 scripts/sot_manager.py --record-output 11 src/crawling/adapters/__init__.py --project-dir .

# 8. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 11 --F <score> --C <score> --L <score> --project-dir .

# 9. Finalize team + Advance
python3 scripts/sot_manager.py --update-team '{"name":"site-adapters-team","status":"all_completed","tasks_completed":["adapter-dev-kr-major","adapter-dev-kr-tech","adapter-dev-english","adapter-dev-multilingual"],"tasks_pending":[],"completed_summaries":{}}' --project-dir .
python3 scripts/validate_step_transition.py --step 11 --project-dir .
python3 scripts/sot_manager.py --advance-step 11 --project-dir .
```

---

### Step 12: Crawling Pipeline Integration & 3-Tier Retry System

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@integration-engineer` (opus) |
| **Output** | `src/crawling/pipeline.py`, `src/crawling/retry_manager.py`, `src/crawling/crawl_report.py` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Verify Steps 10+11 outputs exist (import test)
python3 -c "from src.crawling import crawler, anti_block, dedup, ua_manager; print('Imports OK')"

# 3. Spawn @integration-engineer
#    Task: Create pipeline.py + retry_manager.py + crawl_report.py
#    Implements 4-level retry: NetworkGuard(5) × Standard+TotalWar(2) × Crawler(3) × Pipeline(3) = 90

# 4. Validate
python3 scripts/validate_code_structure.py --step 12 --project-dir .

# 5. Record output
python3 scripts/sot_manager.py --record-output 12 src/crawling/pipeline.py --project-dir .

# 6. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 12 --F <score> --C <score> --L <score> --project-dir .

# 7. Advance
python3 scripts/validate_step_transition.py --step 12 --project-dir .
python3 scripts/sot_manager.py --advance-step 12 --project-dir .
```

---

### Step 13: (team) Analysis Pipeline Stages 1-4 (NLP Foundation)

| Item | Value |
|------|-------|
| **Type** | Team |
| **Team** | `analysis-foundation-team` |
| **Checkpoint** | Dense |
| **Members** | `@preprocessing-dev`, `@feature-extraction-dev`, `@article-analysis-dev`, `@aggregation-dev` |
| **Output** | `src/analysis/stage1_preprocessing.py` through `stage4_aggregation.py` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/extract_pipeline_design_s1_s4.py --project-dir .

# 3. Create team
python3 scripts/sot_manager.py --update-team '{"name":"analysis-foundation-team","status":"partial","tasks_completed":[],"tasks_pending":["preprocessing-dev","feature-extraction-dev","article-analysis-dev","aggregation-dev"],"completed_summaries":{}}' --project-dir .

# 4. Spawn team — Dense Checkpoint Pattern
#    Each member implements one stage with CP-1/CP-2/CP-3

# 5. Update team state after each member completes
python3 scripts/validate_team_state.py --project-dir .

# 6. Team Lead verifies Stage 1→2→3→4 data flow (Parquet schema chaining)

# 7. Validate
python3 scripts/validate_code_structure.py --step 13 --project-dir .

# 8. Record output
python3 scripts/sot_manager.py --record-output 13 src/analysis/stage1_preprocessing.py --project-dir .

# 9. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 13 --F <score> --C <score> --L <score> --project-dir .

# 10. Finalize team + Advance
python3 scripts/sot_manager.py --update-team '{"name":"analysis-foundation-team","status":"all_completed","tasks_completed":["preprocessing-dev","feature-extraction-dev","article-analysis-dev","aggregation-dev"],"tasks_pending":[],"completed_summaries":{}}' --project-dir .
python3 scripts/validate_step_transition.py --step 13 --project-dir .
python3 scripts/sot_manager.py --advance-step 13 --project-dir .
```

---

### Step 14: (team) Analysis Pipeline Stages 5-8 (Signal Detection)

| Item | Value |
|------|-------|
| **Type** | Team |
| **Team** | `analysis-signal-team` |
| **Checkpoint** | Dense |
| **Members** | `@timeseries-dev`, `@cross-analysis-dev`, `@signal-classifier-dev`, `@storage-dev` |
| **Output** | `src/analysis/stage5_timeseries.py` through `stage8_output.py`, `src/storage/` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Pre-processing
python3 scripts/extract_pipeline_design_s5_s8.py --project-dir .

# 3. Create team
python3 scripts/sot_manager.py --update-team '{"name":"analysis-signal-team","status":"partial","tasks_completed":[],"tasks_pending":["timeseries-dev","cross-analysis-dev","signal-classifier-dev","storage-dev"],"completed_summaries":{}}' --project-dir .

# 4. Spawn team — Dense Checkpoint Pattern

# 5. Update team state after each member completes
python3 scripts/validate_team_state.py --project-dir .

# 6. Team Lead verifies Stage 5→6→7→8 data flow + PRD §7 schema compliance

# 7. Validate
python3 scripts/validate_code_structure.py --step 14 --project-dir .
python3 scripts/validate_data_schema.py --step 9 --check-config --project-dir .
# → Verify output Parquet schemas match PRD §7.1

# 8. Record output
python3 scripts/sot_manager.py --record-output 14 src/analysis/stage5_timeseries.py --project-dir .

# 9. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 14 --F <score> --C <score> --L <score> --project-dir .

# 10. Finalize team + Advance
python3 scripts/sot_manager.py --update-team '{"name":"analysis-signal-team","status":"all_completed","tasks_completed":["timeseries-dev","cross-analysis-dev","signal-classifier-dev","storage-dev"],"tasks_pending":[],"completed_summaries":{}}' --project-dir .
python3 scripts/validate_step_transition.py --step 14 --project-dir .
python3 scripts/sot_manager.py --advance-step 14 --project-dir .
```

---

### Step 15: Analysis Pipeline Integration & Memory Management

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@integration-engineer` (opus) |
| **Output** | `src/analysis/pipeline.py` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Verify Stages 1-8 all importable
python3 -c "from src.analysis import stage1_preprocessing, stage2_features, stage3_article_analysis, stage4_aggregation, stage5_timeseries, stage6_cross_analysis, stage7_signals, stage8_output; print('All stages OK')"

# 3. Spawn @integration-engineer
#    Task: Create src/analysis/pipeline.py — sequential orchestrator with memory management
#    gc.collect() between stages, peak < 5GB

# 4. Validate
python3 scripts/validate_code_structure.py --step 15 --project-dir .

# 5. Record output
python3 scripts/sot_manager.py --record-output 15 src/analysis/pipeline.py --project-dir .

# 6. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 15 --F <score> --C <score> --L <score> --project-dir .

# 7. Advance
python3 scripts/validate_step_transition.py --step 15 --project-dir .
python3 scripts/sot_manager.py --advance-step 15 --project-dir .
```

---

### Step 16: End-to-End Testing (44 Sites Full Crawl + Analysis)

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@test-engineer` (opus) |
| **Review** | `@reviewer` |
| **Translation** | `@translator` |
| **Output** | `testing/e2e-test-report.md`, `testing/per-site-results.json` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Spawn @test-engineer
#    Task: Full E2E test — crawl all 44 sites + run 8-stage analysis
#    This is the validation that the system works

# 3. Post-processing
python3 scripts/calculate_success_metrics.py --project-dir .
# → Compute PRD §9.1 metrics: success rate ≥ 80%, articles ≥ 500, dedup ≥ 90%

# 4. Record output
python3 scripts/sot_manager.py --record-output 16 testing/e2e-test-report.md --project-dir .

# 5. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 16 --F <score> --C <score> --L <score> --project-dir .

# 6. L2 Review: @reviewer
python3 .claude/hooks/scripts/validate_review.py --step 16 --project-dir . --check-pacs-arithmetic

# 7. Translation: @translator
python3 .claude/hooks/scripts/validate_translation.py --step 16 --project-dir . --check-pacs --check-sequence

# 8. Advance
python3 scripts/validate_step_transition.py --step 16 --project-dir .
python3 scripts/sot_manager.py --advance-step 16 --project-dir .
```

**Key Metrics (PRD §9.1):**
- Crawling success rate ≥ 80% (≥ 35/44 sites)
- Total articles ≥ 500
- Mandatory fields present in ≥ 99% of articles
- Dedup effectiveness ≥ 90%
- E2E time ≤ 3 hours on M2 Pro
- At least L1 and L2 signals detected in output

---

### Step 17: Automation, Scheduling & Self-Recovery

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@devops-engineer` (opus) |
| **Output** | `scripts/run_daily.sh`, `scripts/run_weekly_rescan.sh`, etc. |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Spawn @devops-engineer
#    Task: cron setup, run_daily.sh, self-recovery, logging, archiving

# 3. Validate
python3 scripts/validate_code_structure.py --step 17 --project-dir .

# 4. Record output
python3 scripts/sot_manager.py --record-output 17 scripts/run_daily.sh --project-dir .

# 5. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 17 --F <score> --C <score> --L <score> --project-dir .

# 6. Advance
python3 scripts/validate_step_transition.py --step 17 --project-dir .
python3 scripts/sot_manager.py --advance-step 17 --project-dir .
```

---

### Step 18: (human) Final System Review & Deployment Approval

| Item | Value |
|------|-------|
| **Type** | Human checkpoint |
| **Command** | `/review-final` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Execute /review-final
#    Validates Steps 9-17, checks PRD §9.1 metrics, presents dashboard

# 3. Options: deploy / rework [step] / retest

# 4. Record decision
python3 scripts/sot_manager.py --record-output 18 autopilot-logs/step-18-decision.md --project-dir .

# 5. Autopilot only: P1 decision log validation (DL1-DL6)
python3 .claude/hooks/scripts/validate_decision_log.py --step 18 --project-dir .

# 6. Autopilot only: record auto-approval
python3 scripts/sot_manager.py --add-auto-approved 18 --project-dir .

# 7. Quality gates — auto-detects autopilot from SOT, runs HQ1/HQ2/HQ3 if enabled
python3 scripts/run_quality_gates.py --step 18 --project-dir .

# 8. Validate (ST7 checks decision log exists when autopilot active) + Advance
python3 scripts/validate_step_transition.py --step 18 --project-dir .
python3 scripts/sot_manager.py --advance-step 18 --project-dir .
```

---

### Step 19: Documentation & Operational Guides

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@doc-writer` (opus) |
| **Review** | `@reviewer` |
| **Translation** | `@translator` |
| **Output** | `README.md`, `docs/operations-guide.md`, `docs/architecture-guide.md` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Spawn @doc-writer
#    Task: Create README.md, operations-guide.md, architecture-guide.md
#    Add docstrings to all major Python modules

# 3. Verify all 3 outputs exist
#    Step 19 produces multiple outputs — verify each before recording sentinel
test -f README.md && test -f docs/operations-guide.md && test -f docs/architecture-guide.md && echo "All 3 outputs exist"

# 4. Record output (sentinel = primary deliverable; L0 checks this file)
python3 scripts/sot_manager.py --record-output 19 docs/operations-guide.md --project-dir .

# 5. Quality Gates (L1 + L1.5)
#    L1 Verification must cover ALL 3 outputs, not just the sentinel
python3 scripts/sot_manager.py --update-pacs 19 --F <score> --C <score> --L <score> --project-dir .

# 6. L2 Review: @reviewer
python3 .claude/hooks/scripts/validate_review.py --step 19 --project-dir . --check-pacs-arithmetic

# 7. Translation: @translator
#    → README.ko.md, docs/operations-guide.ko.md, docs/architecture-guide.ko.md
python3 .claude/hooks/scripts/validate_translation.py --step 19 --project-dir . --check-pacs --check-sequence

# 8. Advance
python3 scripts/validate_step_transition.py --step 19 --project-dir .
python3 scripts/sot_manager.py --advance-step 19 --project-dir .
```

---

### Step 20: Final Code Review

| Item | Value |
|------|-------|
| **Type** | Agent (solo) |
| **Agent** | `@reviewer` (opus) |
| **Translation** | `@translator` |
| **Output** | `review-logs/step-20-review.md` |

**Execution:**

```bash
# 1. Confirm step
python3 scripts/sot_manager.py --read --project-dir .

# 2. Spawn @reviewer (adversarial)
#    Task: Comprehensive codebase review — security, correctness, reliability,
#          performance, completeness (44 sites + 56 techniques)

# 3. Record output
python3 scripts/sot_manager.py --record-output 20 review-logs/step-20-review.md --project-dir .

# 4. Quality Gates (L1 + L1.5)
python3 scripts/sot_manager.py --update-pacs 20 --F <score> --C <score> --L <score> --project-dir .

# 5. Translation: @translator
python3 .claude/hooks/scripts/validate_translation.py --step 20 --project-dir . --check-pacs --check-sequence

# 6. Validate + Advance
python3 scripts/validate_step_transition.py --step 20 --project-dir .
python3 scripts/sot_manager.py --advance-step 20 --project-dir .

# 7. Mark workflow complete
python3 scripts/sot_manager.py --set-status completed --project-dir .

# 8. Final verification
python3 scripts/sot_manager.py --read --project-dir .
# → current_step == 21, status == "completed"
```

---

## Failure Recovery Reference

### Quality Gate Failure Protocol

```
L0 FAIL (file missing/too small)
  → Output was not saved properly
  → Re-run agent, verify file on disk
  → No retry budget consumed

L1 FAIL (Verification criteria not met)
  → python3 .claude/hooks/scripts/validate_retry_budget.py --step N --gate verification --project-dir . --check-and-increment
  → can_retry: true → Abductive Diagnosis → re-run with diagnosis
  → can_retry: false → escalate to user

L1.5 FAIL (pACS RED < 50)
  → python3 .claude/hooks/scripts/validate_retry_budget.py --step N --gate pacs --project-dir . --check-and-increment
  → can_retry: true → Abductive Diagnosis → rework weak dimension
  → can_retry: false → escalate to user

L2 FAIL (Review FAIL verdict)
  → python3 .claude/hooks/scripts/validate_retry_budget.py --step N --gate review --project-dir . --check-and-increment
  → can_retry: true → Abductive Diagnosis → address issues → re-review
  → can_retry: false → escalate to user

HQ FAIL (Human step quality gates — autopilot mode only)
  HQ1: Decision log missing/too small (< 100 bytes)
    → Create/expand autopilot-logs/step-N-decision.md
    → Validate with: python3 .claude/hooks/scripts/validate_decision_log.py --step N --project-dir .
  HQ2: Step not in auto_approved_steps
    → python3 scripts/sot_manager.py --add-auto-approved N --project-dir .
  HQ3: Previous step output missing/too small
    → Re-run previous step, ensure output is recorded in SOT
  ST7: validate_step_transition blocks when autopilot active + decision log missing
    → Create autopilot-logs/step-N-decision.md before transition
```

### Abductive Diagnosis Protocol

```bash
# Step A: Evidence collection
python3 .claude/hooks/scripts/diagnose_context.py --step N --gate {verification|pacs|review} --project-dir .

# Check Fast-Path
# FP1/FP2: deterministic fix → immediate re-run
# FP3: recurring failure → escalate

# Step B: LLM analysis (if not Fast-Path)
# Use evidence bundle + hypothesis priority to determine root cause

# Step C: Validate diagnosis
python3 .claude/hooks/scripts/validate_diagnosis.py --step N --gate {verification|pacs|review} --project-dir .
```

### Retry Budget Limits

| Mode | Max Retries per Gate |
|------|---------------------|
| Standard | 10 |
| ULW active | 15 |

---

## Team Management Reference

### Team Step Protocol

```
1. SOT: update-team → status: partial, tasks_pending: [all members]
2. TeamCreate → spawn all members
3. Each member: work → CP-1 report → CP-2 report → CP-3 final + pACS
4. As members complete:
   a. SOT: update-team → move member from pending to completed + summary
   b. validate_team_state.py → verify consistency
5. All members done:
   a. Team Lead: integration test + merge
   b. SOT: update-team → status: all_completed
   c. Proceed to quality gates
```

### Team Context Isolation

When spawning parallel teammates, enforce file-level isolation:

- Each teammate writes ONLY to their assigned output directory/files (e.g., `src/crawling/adapters/kr_major/` for @adapter-dev-kr-major).
- No teammate may read or modify another teammate's output files during execution.
- Shared inputs (Step 5 architecture, Step 6 strategies) are read-only for all teammates.
- The Team Lead is the only entity that merges outputs and writes to SOT (`--update-team`).
- If a teammate needs data from another teammate's work, it must request via SendMessage to the Team Lead, NOT by reading the other teammate's files directly.

### Dense Checkpoint Pattern (DCP)

For team steps 10, 11, 13, 14:
- **CP-1**: ~30% completion — report architecture/initial implementation
- **CP-2**: ~70% completion — report core functionality
- **CP-3**: 100% completion — final code + tests + self-pACS

Team Lead reviews at each checkpoint via SendMessage. FAIL at any checkpoint → specific feedback + re-work instruction.

---

## Step-Type Quick Map

| Step | Type | Agent/Team | Review | Translation |
|------|------|-----------|--------|-------------|
| 1 | Agent | @site-recon | @fact-checker | @translator |
| 2 | Team | tech-validation-team (3) | — | — |
| 3 | Agent | @crawl-analyst | @fact-checker | @translator |
| 4 | Human | /review-research | — | — |
| 5 | Agent | @system-architect | @reviewer | @translator |
| 6 | Team | crawl-strategy-team (4) | — | — |
| 7 | Agent | @pipeline-designer | @reviewer | @translator |
| 8 | Human | /review-architecture | — | — |
| 9 | Agent | @infra-builder | — | — |
| 10 | Team | crawl-engine-team (4) | — | — |
| 11 | Team | site-adapters-team (4) | — | — |
| 12 | Agent | @integration-engineer | — | — |
| 13 | Team | analysis-foundation-team (4) | — | — |
| 14 | Team | analysis-signal-team (4) | — | — |
| 15 | Agent | @integration-engineer | — | — |
| 16 | Agent | @test-engineer | @reviewer | @translator |
| 17 | Agent | @devops-engineer | — | — |
| 18 | Human | /review-final | — | — |
| 19 | Agent | @doc-writer | @reviewer | @translator |
| 20 | Agent | @reviewer | — | @translator |
