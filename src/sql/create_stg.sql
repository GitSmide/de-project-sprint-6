DROP TABLE IF EXISTS STV2023081248__STAGING.group_log;

CREATE TABLE STV2023081248__STAGING.group_log
(
group_id BIGINT,
user_id BIGINT,
user_id_from BIGINT,
event VARCHAR(30),
event_dt TIMESTAMP(0)
)
ORDER BY group_id, user_id
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);
