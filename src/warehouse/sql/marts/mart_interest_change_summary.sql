-- Mart 1: Interest change summary -- 

CREATE OR REPLACE VIEW mart_interest_change_summary AS
SELECT
    category_june,
    category_july,
    Trending_Type AS trending_type,
    COUNT(*) AS users
FROM analytics_search_behavior
GROUP BY
    category_june,
    category_july,
    Trending_Type;
