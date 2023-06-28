
with user_group_messages as (
    select hg.hk_group_id as hk_group_id,
           count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from REDRUM94YANDEXRU__DWH.h_groups hg
        join REDRUM94YANDEXRU__DWH.l_groups_dialogs  lgd on hg.hk_group_id = lgd.hk_group_id
        join REDRUM94YANDEXRU__DWH.h_dialogs hd on lgd.hk_message_id = hd.hk_message_id
        join REDRUM94YANDEXRU__DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id
    group by hg.hk_group_id
),
user_group_log as (
select luga.hk_group_id as hk_group_id,
       registration_dt,
       count(distinct luga.hk_user_id) as cnt_added_users
from REDRUM94YANDEXRU__DWH.l_user_group_activity luga
    join REDRUM94YANDEXRU__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    join REDRUM94YANDEXRU__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id
where event='add'
group by luga.hk_group_id, registration_dt
order by registration_dt
)

select 
ugl.hk_group_id, 
cnt_users_in_group_with_messages, 
cnt_added_users, 
cnt_users_in_group_with_messages / cnt_added_users as group_conversion
from user_group_log ugl 
join user_group_messages ugm 
on ugl.hk_group_id=ugm.hk_group_id
order by 4 desc