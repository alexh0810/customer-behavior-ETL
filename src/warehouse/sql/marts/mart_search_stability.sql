-- Mart 3 Search stability vs change 

CREATE OR REPLACE VIEW mart_search_stability AS
WITH base AS (
    SELECT
        Trending_Type AS trending_type,
        COUNT(DISTINCT user_id) AS users
    FROM analytics_search_behavior
    GROUP BY Trending_Type
),
total AS (
    SELECT SUM(users) AS total_users FROM base
)
SELECT
    b.trending_type,
    b.users,
    ROUND(b.users * 100.0 / t.total_users, 2) AS percentage
FROM base b
CROSS JOIN total t;
