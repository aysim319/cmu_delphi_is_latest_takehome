# make copy to verify
create table epimetric_latest_copy as (select * from epimetric_latest);

drop table latest_from_full;
create temporary table if not exists latest_from_full as (
    select a.* from epimetric_full a
    inner join (select signal_key_id, geo_key_id, time_type, time_value, max(issue) as latest_issue from epimetric_full
        group by signal_key_id, geo_key_id, time_type, time_value) b
        on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.latest_issue);
select count(*) from latest_from_full;


create temporary table if not exists  matching_latest  as (
    select a.* from epimetric_latest a
    inner join latest_from_full b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.latest_issue);
select count(*) from matching_latest;


drop table to_update;
create temporary table if not exists to_update as (
    select a.* from latest_from_full a
    left join matching_latest b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value
    where b.issue is null);
select count(*) from to_update;


update epimetric_latest_copy a, to_update b
set a.issue = b.issue,
a.value = b.value,
a.stderr = b.stderr,
a.sample_size = b.sample_size,
a.lag = b.lag,
a.value_updated_timestamp = b.value_updated_timestamp,
a.computation_as_of_dt = b.computation_as_of_dt,
a.missing_value = b.missing_value,
a.missing_stderr = b.missing_stderr,
a.missing_sample_size = b.missing_sample_size
where a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value;


select count(*) from to_update a
left join epimetric_latest_copy b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value andcreate table epimetric_latest_copy as (select * from epimetric_latest);

drop table latest_from_full;
create temporary table if not exists latest_from_full as (
    select a.* from epimetric_full a
    inner join (select signal_key_id, geo_key_id, time_type, time_value, max(issue) as latest_issue from epimetric_full
        group by signal_key_id, geo_key_id, time_type, time_value) b
        on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.latest_issue);
select count(*) from latest_from_full;


create temporary table if not exists  matching_latest  as (
    select a.* from epimetric_latest a
    inner join latest_from_full b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.latest_issue);
select count(*) from matching_latest;


drop table to_update;
create temporary table if not exists to_update as (
    select a.* from latest_from_full a
    left join matching_latest b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value
    where b.issue is null);
select count(*) from to_update;


update epimetric_latest_copy a, to_update b
set a.issue = b.issue,
a.value = b.value,
a.stderr = b.stderr,
a.sample_size = b.sample_size,
a.lag = b.lag,
a.value_updated_timestamp = b.value_updated_timestamp,
a.computation_as_of_dt = b.computation_as_of_dt,
a.missing_value = b.missing_value,
a.missing_stderr = b.missing_stderr,
a.missing_sample_size = b.missing_sample_size
where a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value;

# should be 0, (to_update is a subset after updating)
select count(*) from to_update a
left join epimetric_latest_copy b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.issue
where b.issue is null
;

#shoudl be 981/size of to_update
select count(*) from to_update a
left join epimetric_latest b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.issue
where b.issue is null
;




select count(*) from to_update a
left join epimetric_latest b
    on a.signal_key_id = b.signal_key_id and
    a.geo_key_id = b.geo_key_id and
    a.time_type = b.time_type and
    a.time_value = b.time_value and
    a.issue = b.issue
where b.issue is null
;

