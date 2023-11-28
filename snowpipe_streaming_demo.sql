--- set context
use role accountadmin;
use warehouse kafka_wh;
use database snowpipe_streaming;
use schema dev;

--- Let's take a look at the raw streaming records that Snowpipe is ingesting:
select count(*) from snowpipe_streaming.dev.trades_stream; --- see how many records Snowpipe is ingesting
select * from snowpipe_streaming.dev.trades_stream order by timestamp desc; limit 100; --- take a look at the raw XML/FPML landing from Kafka



--- 


--- Let's take a look at the AD -> CAMPAIGN mapping reference data set that we will join with the ad data in our DT
select * from ad_campaign_map;

--- Let's take a look at the Dynamic Table results, which show us daily ad cost, clicks, etc. across channels & campaigns:
select * from campaign_spend order by date desc limit 500;

--- The DT will auto-refresh after a little bit of time.
--- Notice the values have now changed!
select * from campaign_spend order by date desc limit 500;

--- sum(ads_served) compared to count(*) from the raw records should reflect any latency between ingest and DT refresh
select sum(ads_served) from campaign_spend;
select count(*) from streaming_ad_record;
