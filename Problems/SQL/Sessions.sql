/*
Sessions

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
