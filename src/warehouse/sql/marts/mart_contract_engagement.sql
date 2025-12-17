-- Mart 2: Contract engagement 

CREATE OR REPLACE VIEW mart_contract_engagement AS
SELECT
    contract,
    CASE
        WHEN activedays >= 20 THEN 'High'
        WHEN activedays >= 10 THEN 'Medium'
        ELSE 'Low'
    END AS engagement_level,
    most_watched,
    taste
FROM analytics_content_behavior;
 