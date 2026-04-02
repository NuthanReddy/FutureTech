# Agent Routing Guide

Use this quick map to pick the right agent for a task in this repo.

| Agent | Use when user asks about | Typical target folders |
|---|---|---|
| `test-case-generator` | "write tests", "create test cases", "how should I test" | `tests/`, plus touched module folder |
| `build-validation-runner` | "run/validate tests", "check build", "verify changes" | `tests/`, changed Python folders |
| `git-commit-push-guard` | "commit changes", "push branch", "set upstream", "publish commits" | repo git workflow (current branch + scoped staged files) |
| `problem-solution-coach` | "solve this problem", "optimize algorithm", "explain complexity" | `Problems/` |
| `data-structure-implementer` | "add data structure", "implement operations", "extend DS" | `DataStructures/`, `tests/` |
| `bug-triage-debug` | "failing", "error", "unexpected behavior" | changed folder; often `Problems/`, `DataStructures/`, `SystemDesign/` |
| `python-cleanup-refactor` | "cleanup", "refactor", "improve readability" | local target module/folder only |
| `system-design-simulator` | "simulate", "design flow", "rate limiter/load balancer" | `SystemDesign/`, `LLD/` |
| `logger-variant-boundary` | "logging change", "sink/config update", "logger bug" | either `LLD/LoggerModule/logger/` **or** `Utils/` |
| `ai-genai-experiments` | "GenAI", "Google API", "prompt/image experiment" | `AI/` |
| `pyspark-local-jobs` | "spark job", "DataFrame transform in Spark", "local spark" | `PySpark/` |
| `pandas-script-analyst` | "pandas analysis", "clean DataFrame", "csv/data script" | `Pandas/` |

## Fast Rules

- Keep edits scoped to one domain folder unless user explicitly asks for cross-folder work.
- For logger work, do not mix both implementations in one change unless requested.
- For validation, prefer targeted checks before broad test runs.

## Maintenance

When adding a new `*.agent.md`, add one row here with trigger phrases and target folders.
