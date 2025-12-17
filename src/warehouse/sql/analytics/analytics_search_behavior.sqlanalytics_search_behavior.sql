--- User search analytics view

CREATE OR REPLACE VIEW analytics_search_behavior AS
SELECT
    user_id,
    most_search_june,
    most_search_july,
    category_june,
    category_july,
    trending_type,
    previous
FROM fact_search_behavior;
