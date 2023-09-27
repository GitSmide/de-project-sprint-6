DROP TABLE IF EXISTS STV2023081248__DWH.s_auth_history;

CREATE TABLE STV2023081248__DWH.s_auth_history
(
hk_l_user_group_activity BIGINT,
user_id_from BIGINT,
event VARCHAR(30),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2023081248__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event ,load_dt ,load_src)
SELECT DISTINCT
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	now() as load_dt,
	's3' as load_src
from STV2023081248__STAGING.group_log as gl
left join STV2023081248__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023081248__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023081248__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
WHERE luga.hk_l_user_group_activity NOT IN (SELECT hk_l_user_group_activity FROM STV2023081248__DWH.s_auth_history);