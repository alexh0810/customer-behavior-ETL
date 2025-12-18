# src/dashboard/queries.py

SEARCH_STABILITY = """
SELECT trending_type, users, percentage
FROM mart_search_stability
ORDER BY trending_type
"""

INTEREST_TRANSITIONS = """
SELECT
    category_june || ' → ' || category_july AS transition,
    users
FROM mart_interest_change_summary
WHERE trending_type = 'Changed'
ORDER BY users DESC
LIMIT 10
"""

CONTRACT_ENGAGEMENT = """
SELECT engagement_level, COUNT(*) AS contracts
FROM mart_contract_engagement
GROUP BY engagement_level
ORDER BY engagement_level
"""
