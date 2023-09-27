WITH user_group_messages AS (
	SELECT DISTINCT 
		luga.hk_group_id,
		COUNT(hu.hk_user_id) AS cnt_users_in_group_with_messages
	FROM STV2023081248__DWH.l_user_message AS lum
	JOIN STV2023081248__DWH.h_users AS hu ON lum.hk_user_id=hu.hk_user_id
	JOIN STV2023081248__DWH.l_user_group_activity AS luga ON hu.hk_user_id=luga.hk_user_id 
	GROUP BY luga.hk_group_id
),
user_group_log AS 
(
	SELECT DISTINCT
		luga.hk_group_id,
		COUNT(luga.hk_user_id) AS cnt_added_users
	FROM STV2023081248__DWH.l_user_group_activity AS luga 
	WHERE luga.hk_l_user_group_activity IN (
		SELECT sah.hk_l_user_group_activity 
		FROM STV2023081248__DWH.s_auth_history AS sah
		WHERE sah.event = 'add')
	AND luga.hk_group_id IN (
		SELECT hg.hk_group_id
		FROM STV2023081248__DWH.h_groups AS hg
		ORDER BY hg.registration_dt
		LIMIT 10
	)
GROUP BY luga.hk_group_id
)

SELECT 
	ugm.hk_group_id,
	cnt_added_users,
	cnt_users_in_group_with_messages,
	ROUND((cnt_users_in_group_with_messages /  cnt_added_users)*100,2) AS group_conversion
FROM user_group_messages AS ugm
JOIN user_group_log AS ugl ON ugm.hk_group_id=ugl.hk_group_id
ORDER BY group_conversion DESC;