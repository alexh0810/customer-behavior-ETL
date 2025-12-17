 -- Contract content analytics view 

CREATE OR REPLACE VIEW analytics_content_behavior AS
SELECT
    contract,
    most_watched,
    taste,
    activedays,
    totaldevices,
    "Giải Trí",
    "Phim Truyện",
    "Thiếu Nhi",
    "Thể Thao",
    "Truyền Hình"
FROM fact_content_interactions;
