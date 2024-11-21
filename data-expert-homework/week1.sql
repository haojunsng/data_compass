-- Data Engineering Bootcamp: Data Modelling Homework (Week 1)

-- Initial Look at Data
select * from actor_films;

-- Create required structs
create type quality_type as enum ('star', 'good', 'average', 'bad');

create type film_item as (
    film text,
    votes integer,
    rating decimal,
    filmid text
);

-- Q1
drop table if exists actors;
create table actors (
    actorid text,
    films film_item [],
    quality_class quality_type,
    is_active bool,
    current_year integer,
    primary key (actorid, current_year)
);

/*
Q2 (year is hardcoded here but typically we use airflow's template variables)
insert query has been run for years 2011 to 2016
*/
truncate table actors;
insert into actors
select
    actorid,
    array_agg(
        row(
            film,
            votes,
            rating,
            filmid
        )::film_item
    ) as films,
    case
        when avg(rating) > 8 then 'star'
        when avg(rating) > 7 then 'good'
        when avg(rating) > 6 then 'average'
        else 'bad'
    end::quality_type as quality_class,
    coalesce(max(year) = extract(year from current_date), false) as is_active,
    2016 as current_year
from actor_films
where year = 2016
group by actorid;

-- Q3
drop table if exists actors_history_scd;
create table actors_history_scd (
    actorid text,
    quality_class quality_type,
    is_active bool,
    start_year integer,
    end_year integer,
    current_year integer
);

-- Q4
truncate table actors_history_scd;
insert into actors_history_scd
with with_previous as (
    select
        actorid,
        current_year,
        lag(quality_class, 1)
            over (partition by actorid order by current_year)
        as previous_quality_class,
        quality_class,
        lag(is_active, 1)
            over (partition by actorid order by current_year)
        as previous_is_active,
        is_active
    from actors
),

with_change_indicator as (
    select
        *,
        case
            when quality_class <> previous_quality_class then 1
            when is_active <> previous_is_active then 1
            else 0
        end as change_indicator
    from with_previous
),

with_streaks as (
    select
        *,
        sum(change_indicator)
            over (partition by actorid order by current_year)
        as streak_identifier
    from with_change_indicator
)

select
    actorid,
    quality_class::quality_type,
    is_active,
    min(current_year) as start_year,
    max(current_year) as end_year,
    2016 as current_year
from with_streaks
group by actorid, streak_identifier, is_active, quality_class::quality_type
order by actorid;

-- Q5 (For subsequent incremental processing begininng year 2017, at this point actors should have loaded data for year 2017)
create type scd_type as (
    quality_class quality_type,
    is_active bool,
    start_year integer,
    end_year integer
);

insert into actors_history_scd
with last_year_scd as (
    select * from actors_history_scd
    where
        current_year = 2016
        and end_year = 2016
),

historical_scd as ( -- this will not change
    select
        actorid,
        quality_class,
        is_active,
        start_year,
        end_year
    from actors_history_scd
    where
        current_year = 2016
        and end_year < 2016
),

this_season_data as (
    select * from actors
    where current_year = 2017
),

unchanged_records as (
    select
        new.actorid,
        new.quality_class,
        new.is_active,
        old.start_year,
        new.current_year as end_year
    from this_season_data as new
    inner join last_year_scd as old
        on new.actorid = old.actorid
    where
        new.quality_class = old.quality_class
        and new.is_active = old.is_active
),

changed_records as (
    select
        new.actorid,
        unnest(array[
            row(
                old.quality_class,
                old.is_active,
                old.start_year,
                old.end_year
            )::scd_type,
            row(
                new.quality_class,
                new.is_active,
                new.current_year,
                new.current_year
            )::scd_type
        ]) as records
    from this_season_data as new
    left join last_year_scd as old
        on new.actorid = old.actorid
    where (
        new.quality_class <> old.quality_class
        or new.is_active <> old.is_active
    )
),

unnested_changed_records as (
    select
        actorid,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_year,
        (records::scd_type).end_year
    from changed_records
),

new_records as (
    select
        new.actorid,
        new.quality_class,
        new.is_active,
        new.current_year as start_year,
        new.current_year as end_year
    from this_season_data as new
    left join last_year_scd as old
        on new.actorid = old.actorid
    where old.actorid is null
)

select
    *,
    2017 as current_year
from
    (
        select * from historical_scd
        union all
        select * from unchanged_records
        union all
        select * from unnested_changed_records
        union all
        select * from new_records
    ) as final;
