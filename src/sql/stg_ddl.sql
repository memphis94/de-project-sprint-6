--STG DDL

create table REDRUM94YANDEXRU__STAGING.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(9),
    message_from int,
    message_to   int,
    message      varchar(1000),    
    message_type int
)
order by message_id

partition by message_ts::date
group by calendar_hierarchy_day(message_ts::date, 3, 2);


create table REDRUM94YANDEXRU__STAGING.groups
(
    id                  int PRIMARY KEY,
    admin_id            int,
    group_name          varchar (100),
    registration_dt     timestamp(6),
    is_private          boolean
)
order by  id, admin_id
partition by registration_dt::date
group by calendar_hierarchy_day(registration_dt::date, 3, 2);


create table REDRUM94YANDEXRU__STAGING.users
(
    id                  int PRIMARY KEY,
    chat_name           varchar (200),
    registration_dt     timestamp,
    country             varchar (200),
    age                 int
)
order by id;

-- project
create table REDRUM94YANDEXRU__STAGING.group_log
(
    group_id   int PRIMARY KEY,
    user_id   int,
    user_id_from int,
    event   varchar(100),
    datetime      timestamp
)
ORDER BY group_id
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2);


drop table if exists REDRUM94YANDEXRU__STAGING.dialogs;
drop table if exists REDRUM94YANDEXRUREDRUM94YANDEXRU__STAGING.groups;
drop table if exists REDRUM94YANDEXRU__STAGING.users;
drop table if exists REDRUM94YANDEXRU__STAGING.group_log;

