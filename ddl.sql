create table if not exists workspaces
(
    id UUID,
    name String,
    created_at DATETIME64,
    created_by String,
    `date` DATE32
)
ENGINE ReplacingMergeTree()
order by id;

create table if not exists dashboards
(
    guid UUID,
    name String,
    last_modified DATETIME64,
    last_editor_name String,
    dataset String,
    is_public bool,
    published_on_portal bool,
    workspace_id UUID,
    `date` DATE32
)
ENGINE ReplacingMergeTree()
order by guid;

create table if not exists widgets
(
    workspace_id UUID,
    dashboard_id UUID,
    sheet_guid UUID,
    sheet_name String,
    sheet_position UInt8,
    widget_type String,
    widget_label String,
    widget_guid UUID,
    `date` DATE32
)
ENGINE ReplacingMergeTree()
order by (workspace_id, dashboard_id, widget_guid);

create table if not exists roles
(
    `user`` String,
    id UUID,
    subject_type String,
    assigned_role String,
    workspace_id UUID,
    `date` DATE32
)
ENGINE ReplacingMergeTree()
order by id;

create table if not exists aggregates
(
    `date` DATE32,
    aggregate_type String,
    workspace String,
    dashboard String,
    count_value UInt64
)
ENGINE ReplacingMergeTree()
order by (`date`, aggregate_type, workspace, dashboard);

create MATERIALIZED view if not exists mv_workspaces_count TO aggregates as
select `date` as `date`,
       'workspaces by date' as aggregate_type,
       '' as workspace,
       '' as dashboard,
       count(DISTINCT id) as count_value
    from workspaces
    group by `date`;

create MATERIALIZED view if not exists mv_dashboards_count TO aggregates as
select `date` as `date`,
       'dashboards by date' as aggregate_type,
       workspace_id as workspace,
       '' as dashboard,
       count(DISTINCT guid) as count_value
       from dashboards
       group by `date`, workspace_id;

create MATERIALIZED view if not exists mv_widgets_count TO aggregates as
select `date` as `date`,
       'widgets by date' as aggregate_type,
       workspace_id as workspace,
       dashboard_id as dashboard,
       count(DISTINCT widget_guid) as count_value
       from widgets
       group by `date`, workspace_id, dashboard_id;

create MATERIALIZED view if not exists mv_roles_count TO aggregates as
select `date` as `date`,
       'roles by date' as aggregate_type,
       '' as workspace,
       '' as dashboard,
       count(DISTINCT id) as count_value
       from roles
       where subject_type = 'User'
       group by `date`;

create MATERIALIZED view if not exists mv_roles_workspaces_count TO aggregates as
    select `date` as `date`,
           'roles by workspaces and date' as aggregate_type,
           workspace_id as workspace,
           '' as dashboard,
           count(DISTINCT id) as count_value
           from roles
           where subject_type = 'User'
           group by `date`, workspace_id;

create view if not exists v_aggregates as
    select `date` as `date`,
           sum(if(aggregate_type = 'workspaces by date', count_value, Null)) as workspace_count,
           sum(if(aggregate_type = 'dashboards by date', count_value, Null)) as dashboards_count,
           sum(if(aggregate_type = 'roles by date', count_value, Null)) as users_count,
           sum(if(aggregate_type = 'widgets by date', count_value, Null)) as widget_count
    from aggregates
    group by `date`
    order by `date`;

create table kaf_logs
(
    id String,
    timestamp_utc DATETIME64,
    `user` String,
    workspace_id UUID,
    dashboard_id UUID,
    widget_id UUID
)
    ENGINE Kafka('kafka:9092', 'log_topic_2', 'clickhouse_group3', 'JSONEachRow');

create table logs
(
    id String,
    timestamp_utc DATETIME64,
    user String,
    workspace_id UUID,
    dashboard_id UUID,
    widget_id UUID
)
    ENGINE MergeTree()
order by id
partition by toYYYYMMDD(timestamp_utc);

create MATERIALIZED view kafka_to_logs TO logs as
select *
from kaf_logs;

create view v_dashboard_views as
select
    log.id as log_id,
    dateAdd(HOUR, 3, log.timestamp_utc) as log_dttm_msk,
    log.user as user,
    ws.name as workspace_name,
    db.name as dashboard_name,
    ds.sheet_name,
    ds.sheet_position,
    ds.widget_label,
    ds.widget_type
from logs log
inner join workspaces ws
    on log.workspace_id = ws.id
inner join dashboards db
    on log.dashboard_id = db.guid
inner join widgets ds
    on ds.workspace_id = log.workspace_id
       and ds.dashboard_id = log.dashboard_id
       and ds.widget_guid = log.widget_id;