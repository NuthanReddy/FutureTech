/*
Atlassian SQL Interview Questions and Answers

Reference:
https://datalemur.com/blog/atlassian-sql-interview-questions

The examples use PostgreSQL unless a section is explicitly marked Snowflake.
Table and column names are illustrative and can be adapted to the source
schema.
*/


/*
1. Time-Difference / SLA Aggregation

Question:
Calculate the average bug resolution time in hours for each team.

Answer:
Join teams to bugs, exclude unresolved bugs, calculate each resolution
duration, and average the durations by team.
*/

SELECT
    teams.team_id,
    teams.team_name,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (bugs.resolved_at - bugs.created_at))) / 3600,
        2
    ) AS average_resolution_hours
FROM teams
INNER JOIN bugs
    ON bugs.team_id = teams.team_id
WHERE bugs.resolved_at IS NOT NULL
GROUP BY
    teams.team_id,
    teams.team_name
ORDER BY average_resolution_hours;


/*
2. Top-N / Most-Frequently Used Products

Question:
Determine the most frequently used Atlassian product during the current month.

Answer:
Filter to the current month, aggregate usage events by product, and rank
products by event count. DENSE_RANK returns every product tied for the highest
usage count.
*/

WITH product_usage AS (
    SELECT
        product_name,
        COUNT(*) AS usage_count
    FROM product_usage_events
    WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE)
      AND usage_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
    GROUP BY product_name
),
ranked_products AS (
    SELECT
        product_name,
        usage_count,
        DENSE_RANK() OVER (ORDER BY usage_count DESC) AS usage_rank
    FROM product_usage
)
SELECT
    product_name,
    usage_count
FROM ranked_products
WHERE usage_rank = 1
ORDER BY product_name;


/*
3. Window Functions vs. Self-Joins

Question:
Explain the difference between window functions and self-joins in user journey
analysis.

Answer:
Window functions keep each event row while accessing related rows within the
same user partition. LAG and LEAD are well suited to session gaps, running
totals, and period-over-period comparisons.

A self-join combines a table with another copy of itself. It is useful when
matching events by a relationship that is not based only on row order, but it
can create duplicate combinations and is usually more verbose for sequential
journey analysis.

Example: Find the time since each user's previous event.
*/

SELECT
    user_id,
    event_name,
    event_time,
    LAG(event_time) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS previous_event_time,
    event_time - LAG(event_time) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS time_since_previous_event
FROM user_events;


/*
4. Retention / Churn Analysis

Question:
Identify monthly recurring churn patterns across two product datasets.

Answer:
Combine activity from both products, build one row per user and month, and
identify a churn month when a previously active user has no activity in the
following month. This example reports churn by product and activity month.
*/

WITH combined_activity AS (
    SELECT user_id, activity_date, 'Jira' AS product_name
    FROM jira_activity

    UNION ALL

    SELECT user_id, activity_date, 'Confluence' AS product_name
    FROM confluence_activity
),
monthly_activity AS (
    SELECT DISTINCT
        user_id,
        product_name,
        DATE_TRUNC('month', activity_date)::date AS activity_month
    FROM combined_activity
),
activity_with_next_month AS (
    SELECT
        user_id,
        product_name,
        activity_month,
        LEAD(activity_month) OVER (
            PARTITION BY user_id, product_name
            ORDER BY activity_month
        ) AS next_activity_month
    FROM monthly_activity
)
SELECT
    product_name,
    activity_month,
    COUNT(*) AS active_users,
    COUNT(*) FILTER (
        WHERE next_activity_month IS NULL
           OR next_activity_month > activity_month + INTERVAL '1 month'
    ) AS churned_users,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE next_activity_month IS NULL
               OR next_activity_month > activity_month + INTERVAL '1 month'
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS churn_rate_percent
FROM activity_with_next_month
GROUP BY
    product_name,
    activity_month
ORDER BY
    product_name,
    activity_month;


/*
5. Join Semantics

Question:
Explain the different join types and when to use them.

Answer:
- INNER JOIN returns only matching rows from both tables.
- LEFT JOIN returns every left-side row and matching right-side rows.
- RIGHT JOIN is the reverse of LEFT JOIN and is often rewritten as one.
- FULL OUTER JOIN returns matched and unmatched rows from both tables.
- A self-join relates rows within the same table.
- An anti-join returns rows for which no related row exists.

Example anti-join: users who used Jira but never used Confluence.
*/

SELECT DISTINCT
    jira.user_id
FROM jira_activity AS jira
WHERE NOT EXISTS (
    SELECT 1
    FROM confluence_activity AS confluence
    WHERE confluence.user_id = jira.user_id
);


/*
6. Query Optimization / Tuning

Question:
How would you optimize a slow query over a large dataset with complex joins?

Answer:
1. Inspect the query plan and identify scans, expensive joins, and spills.
2. Filter early and enable partition pruning or predicate pushdown.
3. Index frequently filtered and joined columns in index-based databases.
4. Select only required columns instead of using SELECT *.
5. Pre-aggregate large fact tables before joining when possible.
6. Verify join keys have compatible types and appropriate cardinality.
7. Address skewed join keys that overload individual workers.
8. In Snowflake, review micro-partition pruning and add clustering keys only
   when large tables have stable, selective access patterns.
9. Use materialized views or summary tables for repeated expensive metrics.
10. Measure again after each change instead of assuming it improved the query.

Example: Filter and aggregate events before joining to the product dimension.
*/

WITH recent_usage AS (
    SELECT
        product_id,
        COUNT(*) AS usage_count
    FROM product_events
    WHERE event_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY product_id
)
SELECT
    products.product_name,
    recent_usage.usage_count
FROM recent_usage
INNER JOIN products
    ON products.product_id = recent_usage.product_id
ORDER BY recent_usage.usage_count DESC;


/*
7. NULL Handling in Metrics

Question:
How should missing values be handled when calculating KPIs?

Answer:
- COALESCE(value, default) replaces NULL when a meaningful default exists.
- NULLIF(denominator, 0) prevents division-by-zero errors.
- COUNT(*) counts rows, while COUNT(column) counts only non-NULL values.
- AVG(column) ignores NULL values; replacing NULL with zero changes the metric
  and should be done only when NULL genuinely means zero.
*/

SELECT
    team_id,
    COUNT(*) AS total_bugs,
    COUNT(resolved_at) AS resolved_bugs,
    ROUND(
        100.0 * COUNT(resolved_at) / NULLIF(COUNT(*), 0),
        2
    ) AS resolution_rate_percent,
    COALESCE(SUM(customer_impact), 0) AS total_customer_impact
FROM bugs
GROUP BY team_id;


/*
8. Constraints and Indexing Fundamentals

Question:
What is the purpose of a UNIQUE constraint, and what types of indexes exist?

Answer:
A UNIQUE constraint prevents duplicate values in one column or a combination
of columns. A primary key is both unique and non-NULL and commonly identifies
each row.

An index is a data structure that speeds up reads at the cost of additional
storage and write maintenance:
- Clustered index controls the physical row order; a table normally has one.
- Nonclustered or secondary index stores indexed keys separately with row
  locators; a table can have several.
- Composite index covers multiple columns, and column order affects usage.
- Unique index enforces uniqueness while supporting indexed lookup.

Example definitions:
*/

CREATE TABLE product_subscriptions (
    subscription_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    CONSTRAINT unique_user_product UNIQUE (user_id, product_id)
);

CREATE INDEX product_subscriptions_product_started_idx
    ON product_subscriptions (product_id, started_at);


/*
9. Data Modeling Adjacent to SQL

Question:
When should a star schema or snowflake schema be used, and how would you model
user interactions with Atlassian products?

Answer:
A star schema keeps denormalized dimensions directly connected to a fact
table. It is simple for analysts and usually requires fewer joins.

A snowflake schema normalizes dimensions into related subdimensions. It can
reduce duplication and improve governance but increases query complexity.

For product interactions, use an event fact table at one event per user,
product, and timestamp. Connect it to user, product, date, workspace, and
event-type dimensions. Keep stable identifiers in dimensions and event-level
measures and foreign keys in the fact table.

Illustrative fact table:
*/

CREATE TABLE fact_product_interaction (
    interaction_id BIGINT PRIMARY KEY,
    user_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    workspace_key BIGINT NOT NULL,
    event_type_key BIGINT NOT NULL,
    event_date_key INTEGER NOT NULL,
    event_time TIMESTAMP NOT NULL,
    duration_seconds INTEGER,
    properties JSONB
);


/*
10. Semi-Structured Data

Question:
How would you parse JSON data in a large-scale Snowflake pipeline?

Answer:
Load raw JSON into a VARIANT column, preserve the original payload for replay
and auditing, extract frequently queried attributes into typed columns, and
use LATERAL FLATTEN for arrays. Filter before flattening where possible to
reduce row expansion.

Snowflake example:
*/

SELECT
    events.payload:userId::string AS user_id,
    events.payload:product::string AS product_name,
    events.payload:eventTime::timestamp AS event_time,
    attributes.value:key::string AS attribute_name,
    attributes.value:value::string AS attribute_value
FROM raw_product_events AS events,
LATERAL FLATTEN(input => events.payload:attributes) AS attributes
WHERE events.ingested_at >= DATEADD(day, -7, CURRENT_TIMESTAMP());
