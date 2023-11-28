
--- set context
use role accountadmin;
use warehouse kafka_wh;
use database snowpipe_streaming;
use schema dev;

--- create the reference table that our streaming data will be joined with
create or replace table ad_campaign_map (ad_id integer, campaign varchar);
insert into ad_campaign_map (ad_id, campaign) values 
    (1, 'winter_sports'), 
    (2, 'winter_sports'), 
    (3, 'spring_break'), 
    (4, 'memorial_day'), 
    (5, 'youth_in_action'), 
    (6, 'youth_in_action'), 
    (7, 'memorial_day'), 
    (8, 'youth_in_action'), 
    (9, 'spring_break'), 
    (10, 'winter_sports'),
    (11, 'building_community'),
    (12, 'youth_on_course'),
    (13, 'youth_on_course'),
    (14, 'fathers_day'),
    (15, 'fathers_day'),
    (16, 'fathers_day'),
    (17, 'summer_olympics'),
    (18, 'winter_olympics'),
    (19, 'women_in_sports'),
    (20, 'women_in_sports'),
    (21, 'mothers_day'),
    (22, 'super_bowl'),
    (23, 'stanley_cup'),
    (24, 'nba_finals'),
    (25, 'world_series'),
    (26, 'world_cup'),
    (27, 'uefa'),
    (28, 'family_history'),
    (29, 'thanksgiving_football'),
    (30, 'sports_across_cultures');
    
--- Let's create a DT that joins our raw ad data with the campaign info, and calculates the spend/CPC and total click count grouped by day, campaign, and channel
create or replace dynamic table campaign_spend lag = '1 minute' 
    warehouse = KAFKA_WH AS
    select c.campaign, p.channel, to_date(to_timestamp(to_number(p.timestamp))) date,
        sum(p.click) total_clicks,
        sum(p.cost) total_cost,
        sum(1) ads_served
    from
        ad_campaign_map c, streaming_ad_record p where
    c.ad_id = p.ad_id group by
    c.campaign, p.channel, date;

--- If you are not using auto-schema detection and schema creation,
--- uncomment and create both the parsed_streaming_ad_record AND 
--- campaign_spend DTs below:
-- create or replace dynamic table parsed_streaming_ad_record lag = '1 minute' 
--     warehouse = KAFKA_WH AS
--     select as_varchar(k: channel) channel,
--         to_date(to_timestamp(to_number(k:timestamp))) date,
--         to_decimal(k:click) click,
--         to_decimal(k:cost) cost,
--         to_number(k:ad_id) ad_id
--     from (select RECORD_CONTENT as k from streaming_ad_record);

-- create or replace dynamic table campaign_spend lag = '1 minute' 
--     warehouse = KAFKA_WH AS
--     select c.campaign, p.channel, p.date,
--         sum(p.click) total_clicks,
--         sum(p.cost) total_cost,
--         sum(1) ads_served
--     from
--         ad_campaign_map c, parsed_streaming_ad_record p where
--     c.ad_id = p.ad_id group by
--     c.campaign, p.channel, p.date;
    
--- drop the tables if we need to reset the environment
-- drop table streaming_ad_record;
-- drop dynamic table campaign_spend;
-- drop dynamic table parsed_streaming_ad_record;

-- show channels;


--- steps to unload the daily ad spend data to CSV if necessary
-- use role accountadmin;
-- create or replace file format daily_ad_spend_csv
--     type='CSV'
--     field_delimiter = ',';
    
-- create stage daily_ad_spend_unload
--     file_format = daily_ad_spend_csv;
    
--  copy into @daily_ad_spend_unload from
--     snowpipe_streaming.dev.campaign_spend
--     file_format = daily_ad_spend_csv;
    
    
-- list @daily_ad_spend_unload;

--- These queries will create a view of the pivoted channel spend features, and include a 
--- revenue column that is constructed using these spend values, thus creating
--- useful/performant target data for model training. This only needs to be done
--- under certain circumstances related to part 2 of the demo.

-- use role accountadmin;
-- create or replace view pivoted_ad_spend as (select * from (select channel, sum(total_cost) total_cost, year(date) year, month(date) month from campaign_spend group by year, month, channel)
--     pivot(sum(total_cost) for channel in ('social_media', 'search_engine', 'video', 'email')) as p (year, month, social_media, search_engine, video, email) order by year, month asc);

-- create or replace view pivoted_ad_spend_w_revenue as (select year, month, social_media, search_engine, video, email, (2.5*social_media)+(1.75*search_engine)+(0.98*video)+(1.13*email)+uniform(-0.01,0.01, random())*(social_media+search_engine+video+email) as revenue from pivoted_ad_spend);
-- select * from pivoted_ad_spend_w_revenue;

-- drop views
-- drop view pivoted_ad_spend_w_revenue;
-- drop view pivoted_ad_spend;