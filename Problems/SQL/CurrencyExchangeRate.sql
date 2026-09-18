/*
Currency Exchange Rate - Hard

Skills: SQL (Advanced)
Tags: SQL, Simple Joins, Partitioning

The exchange_rate table receives a new row whenever a currency's exchange
rate changes. Return the total sales amount in USD for each sales date,
rounded to two decimal places, including trailing zeros, and ordered by date.

sales_amount
------------
sales_date   sales_amount   currency
2020-01-01   500             INR
2020-01-01   100             GBP
2020-01-02   1000            INR
2020-01-02   500             GBP
2020-01-03   500             INR
2020-01-17   200             GBP

exchange_rate
-------------
source_currency   target_currency   exchange_rate   effective_start_date
INR               USD               0.14            2019-12-31
INR               USD               0.15            2020-01-02
GBP               USD               1.32            2019-12-20
GBP               USD               1.30            2020-01-01
GBP               USD               1.35            2020-01-16

Expected result
---------------
2020-01-01   200.00
2020-01-02   800.00
2020-01-03    75.00
2020-01-17   270.00
*/

WITH rate_periods AS (
    SELECT
        source_currency,
        target_currency,
        exchange_rate,
        CONVERT(date, effective_start_date) AS effective_start_date,
        LEAD(CONVERT(date, effective_start_date)) OVER (
            PARTITION BY source_currency, target_currency
            ORDER BY CONVERT(date, effective_start_date)
        ) AS effective_end_date
    FROM exchange_rate
    WHERE target_currency = 'USD'
)
SELECT
    sales.sales_date,
    CAST(
        ROUND(SUM(sales.sales_amount * rates.exchange_rate), 2)
        AS decimal(18, 2)
    ) AS sales_usd
FROM sales_amount AS sales
INNER JOIN rate_periods AS rates
    ON rates.source_currency = sales.currency
    AND CONVERT(date, sales.sales_date) >= rates.effective_start_date
    AND (
        rates.effective_end_date IS NULL
        OR CONVERT(date, sales.sales_date) < rates.effective_end_date
    )
GROUP BY sales.sales_date
ORDER BY CONVERT(date, sales.sales_date);
