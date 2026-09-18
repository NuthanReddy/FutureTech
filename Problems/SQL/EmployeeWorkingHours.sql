/*
Employee Working Hours - Hard

Skills: SQL (Advanced)
Tags: Windowing, SQL, Simple Joins, Analytic Functions
Dialect: MySQL 8.0+

Calculate the hours, minutes, and seconds between each valid In and Out pair
for every employee.

An employee can have duplicate consecutive punch types. The final punch in a
consecutive group is used before pairing an In with the following Out. An In
without a subsequent Out and an Out without a preceding In are omitted.

The result must be ordered by employee ID and clock-in date/time.

Schema
------
department_master (
    department_id INT PRIMARY KEY,
    department VARCHAR(255) NOT NULL
)

designation_master (
    designation_id INT PRIMARY KEY,
    designation VARCHAR(255) NOT NULL
)

employee_master (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(255) NOT NULL,
    department_id INT,
    designation_id INT
)

employee_attendance (
    employee_id INT,
    at_date DATE NOT NULL,
    at_time VARCHAR(30) NOT NULL,
    punch_type VARCHAR(5)
)

Sample data
-----------
department_master
department_id   department
1               Accounts
2               Human Resource

designation_master
designation_id   designation
1                Manager
2                Sr. Manager

employee_master
employee_id   employee_name       department_id   designation_id
1             Sunil Kumar Goel    2               1
2             Kamli  Dawar        1               2

employee_attendance
employee_id   at_date      at_time   punch_type
1             2021-02-01   08:00     In
2             2021-02-01   08:10     In
1             2021-02-01   11:30     Out
1             2021-02-01   11:35     Out
1             2021-02-01   12:45     In
2             2021-02-01   16:45     Out
1             2021-02-01   17:30     Out
1             2021-02-01   01:00     Out

Expected output
---------------
employee_id   employee_name      department       designation   in_at_date   in_at_time      out_at_date   out_at_time     working_hours
1             Sunil Kumar Goel   Human Resource   Manager       2021-02-01   08:00:00.000    2021-02-01    11:35:00.000   03:35:00
1             Sunil Kumar Goel   Human Resource   Manager       2021-02-01   12:45:00.000    2021-02-01    17:30:00.000   04:45:00
2             Kamli  Dawar       Accounts         Sr. Manager   2021-02-01   08:10:00.000    2021-02-01    16:45:00.000   08:35:00
*/

WITH normalized_attendance AS (
    SELECT
        employee_id,
        TIMESTAMP(
            at_date,
            STR_TO_DATE(TRIM(at_time), '%H:%i')
        ) AS punched_at,
        TRIM(punch_type) AS punch_type
    FROM employee_attendance
    WHERE TRIM(punch_type) IN ('In', 'Out')
),
punches_with_next_type AS (
    SELECT
        employee_id,
        punched_at,
        punch_type,
        LEAD(punch_type) OVER (
            PARTITION BY employee_id
            ORDER BY punched_at
        ) AS next_punch_type
    FROM normalized_attendance
),
deduplicated_punches AS (
    SELECT
        employee_id,
        punched_at,
        punch_type
    FROM punches_with_next_type
    WHERE next_punch_type IS NULL
       OR next_punch_type <> punch_type
),
punch_pairs AS (
    SELECT
        employee_id,
        punched_at AS clock_in_at,
        punch_type,
        LEAD(punched_at) OVER (
            PARTITION BY employee_id
            ORDER BY punched_at
        ) AS clock_out_at,
        LEAD(punch_type) OVER (
            PARTITION BY employee_id
            ORDER BY punched_at
        ) AS clock_out_type
    FROM deduplicated_punches
)
SELECT
    pairs.employee_id,
    employees.employee_name,
    departments.department,
    designations.designation,
    DATE(pairs.clock_in_at) AS in_at_date,
    DATE_FORMAT(pairs.clock_in_at, '%H:%i:%s.000') AS in_at_time,
    DATE(pairs.clock_out_at) AS out_at_date,
    DATE_FORMAT(pairs.clock_out_at, '%H:%i:%s.000') AS out_at_time,
    TIME_FORMAT(
        SEC_TO_TIME(
            TIMESTAMPDIFF(
                SECOND,
                pairs.clock_in_at,
                pairs.clock_out_at
            )
        ),
        '%H:%i:%s'
    ) AS working_hours
FROM punch_pairs AS pairs
INNER JOIN employee_master AS employees
    ON employees.employee_id = pairs.employee_id
INNER JOIN department_master AS departments
    ON departments.department_id = employees.department_id
INNER JOIN designation_master AS designations
    ON designations.designation_id = employees.designation_id
WHERE pairs.punch_type = 'In'
  AND pairs.clock_out_type = 'Out'
ORDER BY
    pairs.employee_id,
    pairs.clock_in_at;
