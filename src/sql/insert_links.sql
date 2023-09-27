INSERT INTO STV2023081248__DWH.l_user_group_activity (hk_l_user_group_activity, hk_group_id, hk_user_id ,load_dt ,load_src)
SELECT DISTINCT
	hash(hk_group_id, hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
FROM STV2023081248__STAGING.group_log AS gl
LEFT JOIN STV2023081248__DWH.h_users AS hu ON hu.user_id=gl.user_id
LEFT JOIN STV2023081248__DWH.h_groups AS hg ON hg.group_id=gl.group_id
WHERE hash(hk_group_id, hk_user_id) NOT IN (SELECT hk_l_user_group_activity FROM STV2023081248__DWH.l_user_group_activity)