# SQL

Relational queries, ordered-event analysis, and schema exercises.
Dialects: Atlassian uses PostgreSQL except §10 (Snowflake); currency rates use
SQL Server syntax; employee hours use MySQL 8+.

## Pattern identification steps

1. Identify the output grain and cues: totals, ranked groups, consecutive events, or effective dates.
2. Choose GROUP BY for collapsed rows, windows for adjacent rows/ranks, and joins for related entities.
3. Keep joins at the intended grain; define partition keys, event order, ties, NULLs, and interval boundaries.
4. Check temporal completeness before interpreting missing activity as churn; keep dialect-specific functions consistent.
5. Avoid windows for unordered totals and self-joins for simple adjacency; use an anti-join for missing matches.

## Problem cues → patterns

| Problem | Identification cue → pattern |
| --- | --- |
| [Currency exchange](CurrencyExchangeRate.sql) | Rate changes over time → LEAD-built half-open validity intervals + temporal join + daily SUM. |
| [Employee hours](EmployeeWorkingHours.sql) | Repeated In/Out punches → retain each run's last punch, then LEAD-pair In→Out per employee. |
| [Sessions](Sessions.sql) | Consecutive users in global event order → LAG boundary flag + cumulative SUM + island aggregation, not inactivity sessions. |
| [Atlassian §1](AtlassianInterviewQuestions.sql) | Average completed resolution duration per team → filtered join + duration AVG. |
| Atlassian §2 | Highest monthly usage, including ties → month filter + grouped COUNT + DENSE_RANK. |
| Atlassian §3 | Time since a user's previous event → partitioned, time-ordered LAG. |
| Atlassian §4 | Active month without next-month return → distinct user/product/month grain + LEAD gap + conditional counts. |
| Atlassian §5 | Jira users never seen in Confluence → correlated NOT EXISTS anti-join. |
| Atlassian §6 | Large event facts joined to product names → filter + pre-aggregate before dimension join. |
| Atlassian §7 | Missing values and rate denominators → COUNT(column), COALESCE, and NULLIF-aware aggregation. |
| Atlassian §8 | Unique subscriptions and product/date lookup → composite UNIQUE constraint + ordered composite index. |
| Atlassian §9 | Analytics at one interaction per row → event-grain fact table + dimension keys. |
| Atlassian §10 | Attributes nested in JSON arrays → typed VARIANT extraction + LATERAL FLATTEN (Snowflake). |
