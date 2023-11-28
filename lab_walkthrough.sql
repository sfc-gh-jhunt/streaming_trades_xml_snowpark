
--- set context
use role sysadmin;
use warehouse hol_wh;
use database trades;
use schema raw;

-- Check the data is streaming in to our raw.trades table
select * from  trades_stream;
select count(*) from trades_stream;

-- Example of how to convert to JSON first and query using dot notation?

-- Then move on to extraction from XML (Nick's content)

-- Then how to build out a pipeline (stream/task or dynamic table) to process the incoming rows automatically
