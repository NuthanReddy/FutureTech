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
Pattern identification: completed resolution averages -> filtered duration AVG by team.

Question:
Calculate the average bug resolution time in hours for each team.

Answer:
Join teams to bugs, exclude unresolved bugs, calculate each resolution
duration, and average the durations by team.
*/

-- 1. Output: One row per team with completed bugs: average resolution hours,
--    rounded to two decimals and ordered from shortest average to longest.
-- 2. Structure: Many bugs belong to one team, and two timestamps give each
--    duration; grouping is needed because the answer has one row per team, not per bug.
-- 3. Constraints: Assume unique team IDs and valid timestamps; unresolved bugs
--    and teams with no completed bugs are excluded; AVG ignores NULL durations.
-- 4. Choice: Join on team_id, filter resolved_at, then group by team and
--    average elapsed seconds before converting to hours.
-- 5. Why it works: Each matching completed bug contributes one duration;
--    grouping preserves the team grain without multiplying bugs under unique keys.
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
Pattern identification: monthly leader with ties -> grouped COUNT + DENSE_RANK.

Question:
Determine the most frequently used Atlassian product during the current month.

Answer:
Filter to the current month, aggregate usage events by product, and rank
products by event count. DENSE_RANK returns every product tied for the highest
usage count.
*/

-- 1. Output: Return every product tied for the most usage events this month,
--    with its event count, ordered by product name.
-- 2. Structure: Many event rows belong to each product; first count them,
--    then compare product totals, retaining every product sharing the largest count.
-- 3. Constraints: Include the month's start but exclude next month's start;
--    NULL dates fail the filter, while NULL product names form one group.
-- 4. Choice: Count filtered events per product, then DENSE_RANK those counts
--    descending and keep rank 1.
-- 5. Why it works: Ranking aggregated counts compares products rather than
--    individual events, and shared rank 1 retains all leaders instead of breaking ties.
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
Pattern identification: previous event per user -> time-ordered, partitioned LAG.

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

-- 1. Output: Keep one row per event with the same user's preceding timestamp
--    and the elapsed interval since it; the first event's two added fields are NULL.
-- 2. Structure: Each event needs only the same user's immediately previous
--    event, making ordered LAG more direct than joining every possible event pair.
-- 3. Constraints: Assume non-NULL timestamps and no within-user time ties;
--    ties lack a tie-breaker here, and window ordering does not sort final output.
-- 4. Choice: Partition LAG by user_id and order by event_time; subtract the
--    preceding time from the current one without collapsing event rows.
-- 5. Why it works: LAG accesses only the immediate predecessor in that user's
--    ordered partition, so other users never supply the previous event.
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
Pattern identification: missing next-month return -> distinct monthly activity + LEAD gap.
Treat trailing months as churn only when the next observation month is complete.

Question:
Identify monthly recurring churn patterns across two product datasets.

Answer:
Combine activity from both products, build one row per user and month, and
identify a churn month when a previously active user has no activity in the
following month. This example reports churn by product and activity month.
*/

-- 1. Output: One row per product and active month with active-user count,
--    next-month churn count, and percentage; a later return can still follow churn.
-- 2. Structure: Repeated events should count as one active user/product/month;
--    the next active month reveals whether the immediately following month was missed.
-- 3. Constraints: Assume non-NULL users/dates and a complete next-month observation;
--    the query still counts trailing NULL next months as churn without checking completeness.
-- 4. Choice: Combine sources with product labels, deduplicate user/product/month,
--    and use LEAD per user/product to count missing or skipped next months.
-- 5. Why it works: Distinct monthly rows count each user once per product/month;
--    only a return exactly one month later avoids churn; NULLIF protects the rate divisor.
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
Pattern identification: entities with no matching activity -> NOT EXISTS anti-join.

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

-- 1. Output: Return distinct Jira user IDs with no equal user ID in Confluence.
-- 2. Structure: We need absence of any Confluence match, not its event count;
--    NOT EXISTS answers that question without multiplying repeated Jira events.
-- 3. Constraints: No date filter is applied; assume non-NULL IDs for person matching.
--    A NULL Jira ID survives because equality to NULL never establishes a match.
-- 4. Choice: Use correlated NOT EXISTS to reject any Confluence match, then
--    DISTINCT to collapse repeated Jira occurrences.
-- 5. Why it works: Existence checks do not multiply Jira rows, and one matching
--    Confluence event is enough to exclude the user regardless of event counts.
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
Pattern identification: large fact-to-dimension metric -> filter and pre-aggregate by join key.

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

-- 1. Output: One matched product-ID row with its product name and recent event
--    count, ordered descending; identical names are not merged.
-- 2. Structure: Many events share a product_id, but only its total is needed;
--    counting first reduces the rows that must join to product names.
-- 3. Constraints: Assume unique dimension IDs; the lower date bound is inclusive,
--    with no upper bound, so future events qualify; NULL times/keys cannot match.
-- 4. Choice: Filter events from CURRENT_DATE minus 30 days, count per product_id,
--    then join those smaller grouped results to the dimension.
-- 5. Why it works: Each count is computed before the join; unique dimension keys
--    preserve it, while missing products disappear and equal counts have no tie order.
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
Pattern identification: missing metrics and rate denominators -> NULL-aware aggregates.

Question:
How should missing values be handled when calculating KPIs?

Answer:
- COALESCE(value, default) replaces NULL when a meaningful default exists.
- NULLIF(denominator, 0) prevents division-by-zero errors.
- COUNT(*) counts rows, while COUNT(column) counts only non-NULL values.
- AVG(column) ignores NULL values; replacing NULL with zero changes the metric
  and should be done only when NULL genuinely means zero.
*/

-- 1. Output: One row per team_id with total/resolved bug counts, resolved
--    percentage, and total recorded customer impact (zero if every impact is NULL).
-- 2. Structure: One team has many bugs, and missing resolution/impact values
--    affect different metrics differently; grouping and NULL-aware counts fit this output.
-- 3. Constraints: NULL resolved_at means unresolved; NULL impacts are ignored
--    by SUM, and NULL team IDs group together; no empty-team rows are generated.
-- 4. Choice: GROUP BY team_id; compare COUNT(resolved_at) to COUNT(*),
--    guard the denominator with NULLIF, and default a NULL impact sum with COALESCE.
-- 5. Why it works: Each bug contributes to the total but only non-NULL resolutions
--    enter the numerator; decimal scaling yields percentages rather than integer ratios.
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
Pattern identification: unique user/product pairs + dated lookup -> UNIQUE + composite index.

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

-- 1. Output: Define a subscription table enforcing row IDs and unique
--    user/product pairs, plus an index for product-and-start-time lookups.
-- 2. Structure: Writes must reject repeated user/product pairs, while reads
--    search by product and start time; these are constraint/index needs, not ranking.
-- 3. Constraints: IDs and timestamps cannot be NULL; a user/product pair cannot
--    recur even at another start time; the extra index costs storage and write work.
-- 4. Choice: Declare a primary key and composite UNIQUE constraint, then create
--    an index ordered by product_id followed by started_at.
-- 5. Why it works: Database constraints reject conflicting writes; the index
--    arranges keys for product-first access, but does not guarantee any SELECT row order.
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
Pattern identification: interaction analytics -> event-grain fact with dimension keys.

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

-- 1. Output: Define an illustrative event fact table with a unique interaction
--    ID, dimension-key columns, timestamp, optional duration, and JSONB properties.
-- 2. Structure: Each interaction needs its own record and links to descriptive
--    entities, so an event fact table stores measurements beside dimension identifiers.
-- 3. Constraints: Only interaction_id uniqueness is enforced; dimension keys/time
--    are non-NULL, duration/properties may be NULL, and no foreign keys are declared.
-- 4. Choice: Store one fact row per recorded interaction with dimension identifiers
--    and event-level measurements; this defines storage rather than an algorithm.
-- 5. Why it works: A primary key distinguishes fact rows, but repeated
--    user/product/time combinations and missing dimension references remain possible.
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
Pattern identification: JSON attribute arrays -> typed extraction + LATERAL FLATTEN.

Question:
How would you parse JSON data in a large-scale Snowflake pipeline?

Answer:
Load raw JSON into a VARIANT column, preserve the original payload for replay
and auditing, extract frequently queried attributes into typed columns, and
use LATERAL FLATTEN for arrays. Filter before flattening where possible to
reduce row expansion.

Snowflake example:
*/

-- 1. Output: One row per flattened attribute entry with event fields and
--    attribute name/value, not one row per event.
-- 2. Structure: Event fields sit beside an array of attributes; asking for each
--    attribute requires expanding that array into rows, not grouping events.
-- 3. Constraints: Ingestion at or after the seven-day cutoff qualifies; no upper
--    bound is used. Missing/empty arrays emit no rows by default; absent fields can be NULL.
-- 4. Choice: Use LATERAL FLATTEN for each event's attributes, cast JSON values
--    to the requested types, and filter events by ingested_at.
-- 5. Why it works: Lateral expansion keeps attributes attached to their own
--    event; many attributes repeat event fields, and invalid casts can fail the query.
SELECT
    events.payload:userId::string AS user_id,
    events.payload:product::string AS product_name,
    events.payload:eventTime::timestamp AS event_time,
    attributes.value:key::string AS attribute_name,
    attributes.value:value::string AS attribute_value
FROM raw_product_events AS events,
LATERAL FLATTEN(input => events.payload:attributes) AS attributes
WHERE events.ingested_at >= DATEADD(day, -7, CURRENT_TIMESTAMP());
