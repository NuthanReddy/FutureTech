/*
Sessions
Pattern identification: consecutive equal users -> LAG boundaries + cumulative SUM islands.
Use global event order, not user partitions; this is not inactivity-gap sessionization.

Group consecutive records belonging to the same user into sessions, then
return the start and end time for each session.

Sample input
------------
user_id   event_time
u1        t1
u1        t2
u2        t3
u2        t4
u3        t5
u1        t6
u1        t7
u4        t8

Expected output
---------------
user_id   session_start   session_end
u1        t1              t2
u2        t3              t4
u3        t5              t5
u1        t6              t7
u4        t8              t8
*/

-- 1. Output: One row per consecutive same-user run with its first and last
--    event time, in global run order; a returning user may have multiple rows.
-- 2. Structure: Switching users ends a run, even if that user returns later;
--    compare global neighbors and number changes so separate visits are not merged.
-- 3. Constraints: Assume distinct non-NULL event times; tied times have no
--    tie-breaker, and NULL user IDs start new runs because NULL equality is unknown.
-- 4. Choice: LAG the user globally, flag changes, cumulatively sum flags with
--    a ROWS frame, and group each numbered run to get MIN/MAX timestamps.
-- 5. Why it works: The running session number changes exactly at a boundary;
--    grouping by it keeps separated visits apart even when their user IDs match.
WITH previous_events AS (
    SELECT
        user_id,
        event_time,
        LAG(user_id) OVER (ORDER BY event_time) AS previous_user_id
    FROM sessions
),
session_boundaries AS (
    SELECT
        user_id,
        event_time,
        CASE
            WHEN previous_user_id = user_id THEN 0
            ELSE 1
        END AS starts_new_session
    FROM previous_events
),
numbered_sessions AS (
    SELECT
        user_id,
        event_time,
        SUM(starts_new_session) OVER (
            ORDER BY event_time
            ROWS UNBOUNDED PRECEDING
        ) AS session_id
    FROM session_boundaries
)
SELECT
    user_id,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end
FROM numbered_sessions
GROUP BY
    session_id,
    user_id
ORDER BY session_id;
