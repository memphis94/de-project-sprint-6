--sprint6_lessons

create table REDRUM94YANDEXRU__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);
;

create table REDRUM94YANDEXRU__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);
;

create table REDRUM94YANDEXRU__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      int,
    message_ts datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
segmented by hk_message_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


insert into REDRUM94YANDEXRU__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from REDRUM94YANDEXRU__STAGING.users
where hash(id) not in (select hk_user_id from REDRUM94YANDEXRU__DWH.h_users);

insert into REDRUM94YANDEXRU__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from REDRUM94YANDEXRU__STAGING.groups
where hash(id) not in (select hk_group_id from REDRUM94YANDEXRU__DWH.h_groups);


insert into REDRUM94YANDEXRU__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id as message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from REDRUM94YANDEXRU__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from REDRUM94YANDEXRU__DWH.h_dialogs);

create table REDRUM94YANDEXRU__DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id      bigint not null CONSTRAINT fk_l_user_message_user REFERENCES REDRUM94YANDEXRU__DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES REDRUM94YANDEXRU__DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table REDRUM94YANDEXRU__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES REDRUM94YANDEXRU__DWH.h_dialogs (hk_message_id),
hk_group_id      bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES REDRUM94YANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_l_groups_dialogs all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


create table REDRUM94YANDEXRU__DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_user_id      bigint not null CONSTRAINT fk_l_admins_user REFERENCES REDRUM94YANDEXRU__DWH.h_users (hk_user_id),
hk_group_id      bigint not null CONSTRAINT fk_l_admins_group REFERENCES REDRUM94YANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_l_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.l_user_message(hk_l_user_message, hk_message_id, hk_user_id,load_dt,load_src)
select
       hash(hd.hk_message_id,hu.hk_user_id),
       hd.hk_message_id,
       hu.hk_user_id,
       now() as load_dt,
       's3' as load_src
from REDRUM94YANDEXRU__STAGING.dialogs as d
left join REDRUM94YANDEXRU__DWH.h_users as hu on d.message_from = hu.user_id
left join REDRUM94YANDEXRU__DWH.h_dialogs as hd on d.message_id = hd.message_id
where  hash(hd.hk_message_id,hu.hk_user_id) not in (select hk_l_user_message from REDRUM94YANDEXRU__DWH.l_user_message)
and hu.hk_user_id is not null and hd.hk_message_id is not null;


insert into REDRUM94YANDEXRU__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id,hk_group_id,load_dt,load_src)
select
       hash(hd.hk_message_id,hg.hk_group_id),
       hd.hk_message_id,
       hg.hk_group_id,
       now() as load_dt,
       's3' as load_src
from REDRUM94YANDEXRU__STAGING.dialogs as d
left join REDRUM94YANDEXRU__DWH.h_groups as hg on d.message_type = hg.group_id
left join REDRUM94YANDEXRU__DWH.h_dialogs as hd on d.message_id = hd.message_id
where d.message_type is not null and hash(hd.hk_message_id,hg.hk_group_id) not in (select hk_l_groups_dialogs from REDRUM94YANDEXRU__DWH.l_groups_dialogs);


insert into REDRUM94YANDEXRU__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from REDRUM94YANDEXRU__STAGING.groups as g
left join REDRUM94YANDEXRU__DWH.h_users as hu on g.admin_id = hu.user_id
left join REDRUM94YANDEXRU__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from REDRUM94YANDEXRU__DWH.l_admins);

create table REDRUM94YANDEXRU__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES REDRUM94YANDEXRU__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from REDRUM94YANDEXRU__DWH.l_admins as la
left join REDRUM94YANDEXRU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


create table REDRUM94YANDEXRU__DWH.s_user_socdem
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES REDRUM94YANDEXRU__DWH.h_users (hk_user_id),
country varchar,
age int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select
    hu.hk_user_id,
    u.country as country,
    u.age as age,
    now() as load_dt,
    's3' as load_src
from REDRUM94YANDEXRU__DWH.h_users as hu
left join REDRUM94YANDEXRU__STAGING.users as u on hu.user_id = u.id;


create table REDRUM94YANDEXRU__DWH.s_user_chatinfo
(
hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES REDRUM94YANDEXRU__DWH.h_users (hk_user_id),
chat_name varchar,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
select
    hu.hk_user_id,
    u.chat_name as chat_name,
    now() as load_dt,
    's3' as load_src
from REDRUM94YANDEXRU__DWH.h_users as hu
left join REDRUM94YANDEXRU__STAGING.users as u on hu.user_id = u.id;


drop table if exists REDRUM94YANDEXRU__DWH.s_group_private_status;
create table REDRUM94YANDEXRU__DWH.s_group_private_status
(
hk_group_id bigint not null, --CONSTRAINT fk_s_group_private_status_l_admins REFERENCES REDRUM94YANDEXRU__DWH.l_admins (hk_group_id),
is_private boolean,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
select
    hg.hk_group_id,
    g.is_private as is_private,
    now() as load_dt,
    's3' as load_src
from REDRUM94YANDEXRU__DWH.h_groups as hg
left join REDRUM94YANDEXRU__STAGING.groups as g on hg.group_id = g.id;

create table REDRUM94YANDEXRU__DWH.s_group_name
(
hk_group_id bigint not null,
group_name varchar(100),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select
    hg.hk_group_id,
    g.group_name as group_name,
    now() as load_dt,
    's3' as load_src
from REDRUM94YANDEXRU__DWH.h_groups as hg
left join REDRUM94YANDEXRU__STAGING.groups as g on hg.group_id = g.id;


create table REDRUM94YANDEXRU__DWH.s_dialog_info
(
hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES REDRUM94YANDEXRU__DWH.h_dialogs (hk_message_id),
message varchar(1000),
message_from int,
message_to int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_message_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_dialog_info(hk_message_id, message, message_from, message_to,load_dt,load_src)
select
    hd.hk_message_id,
    message,
    message_from,
    message_to,
    now() as load_dt,
    's3' as load_src
from REDRUM94YANDEXRU__DWH.h_dialogs as hd
left join REDRUM94YANDEXRU__STAGING.dialogs as d on hd.message_id = d.message_id;

--sprint6_project

drop table if exists REDRUM94YANDEXRU__DWH.l_user_group_activity cascade ;
create table REDRUM94YANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id      bigint not null CONSTRAINT fk_l_user_group_activity_h_users REFERENCES REDRUM94YANDEXRU__DWH.h_users  (hk_user_id),
hk_group_id     bigint not null CONSTRAINT fk_l_user_group_activity_h_groups REFERENCES REDRUM94YANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
hash(hg.hk_group_id,hu.hk_user_id) as hk_l_user_group_activity,
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from REDRUM94YANDEXRU__STAGING.group_log as gl
left join REDRUM94YANDEXRU__DWH.h_users hu on gl.user_id= hu.user_id
left join REDRUM94YANDEXRU__DWH.h_groups hg on gl.group_id = hg.group_id;

drop table if exists REDRUM94YANDEXRU__DWH.s_auth_history;
create table REDRUM94YANDEXRU__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES REDRUM94YANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by user_id_from all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into REDRUM94YANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event, event_dt,load_dt,load_src)
select
luga.hk_l_user_group_activity,
gl.user_id_from as user_id_from,
gl.event as event,
gl.event_datetime as event_dt,
now() as load_dt,
's3' as load_src
from REDRUM94YANDEXRU__DWH.l_user_group_activity as luga
left join REDRUM94YANDEXRU__DWH.h_users as hu on luga.hk_user_id = hu.hk_user_id
left join REDRUM94YANDEXRU__DWH.h_groups as hg on luga.hk_group_id = hg.hk_group_id
left join REDRUM94YANDEXRU__STAGING.group_log as gl on hu.user_id = gl.user_id and hg.group_id=gl.group_id;