
--- set context
use role sysadmin;
use warehouse hol_wh;
use database trades;
use schema raw;

-- If not already done, start the Kafka stream into Snowpipe Streaming
-- Check the data is streaming in to our raw.trades table
select count(*) from trades_stream ;
select * from  trades_stream order by timestamp desc limit 100;

-- One option is to convert the whole XML schema to JSON and query using dot notation:
-- xmltodict package can be called in a udf to achieve this.
create or replace function public.xml2json(xml_string varchar)
returns variant
language python
volatile
runtime_version = '3.8'
packages = ('xmltodict')
handler = 'xml2json'
as
$$
import xmltodict
import json

def xml2json(xml_string):
    return xmltodict.parse(xml_string)
    ## return xmltodict.parse(xml_string, process_namespaces=True)
$$;

-- The udf can be called directly in a query:
-- Here you can compare the original XML vs. transformed JSON
select 
record_metadata,
trade_msg,
xml2json(trade_msg) as trade_msg_json
from trades.raw.trades_stream
limit 100;

-- Or used to populate a transformed table containing the JSON:
create or replace table trades.transformed.trades_json as (
select 
record_metadata,
xml2json(trade_msg) as trade_msg_json
from trades.raw.trades_stream)
;

-- Once in JSON format dot-notation can be used to extract the required values as separate columns:
-- however depending on the complexity of the XML it may be necessary to use techniques such as LATERAL FLATTEN to reference the required items:
select
trade_msg_json,
trade_msg_json:"eventActivityReport":"correlationId":"#text"::varchar as correlationId,
trade_msg_json:"eventActivityReport":"header":"messageId":"#text"::varchar as messageId,
MAX(case when party.value:"@id"='party1' then party.value:"partyId":"#text"::varchar else NULL end) as party1,
MAX(case when party.value:"@id"='party2' then party.value:"partyId":"#text"::varchar else NULL end) as party2,
MAX(case when party.value:"@id"='clearingDCO' then party.value:"partyId":"#text"::varchar else NULL end) as party_clearingDCO,
MAX(case when party.value:"@id"='clearingBroker1' then party.value:"partyId":"#text"::varchar else NULL end) as party_clearingBroker1,
MAX(case when party.value:"@id"='tradeSource' then party.value:"partyId":"#text"::varchar else NULL end) as party_tradeSource
 from trades.transformed.trades_json tj,
 LATERAL FLATTEN(INPUT => trade_msg_json:"eventActivityReport":"party") party
 GROUP BY ALL
;


-- Alternatively, selected content can be extracted from the XML by referencing specific tags, and returning in a JSON array:
create or replace function public.udf_parse_xml(content varchar)
returns variant
language python
runtime_version = '3.8'
handler = 'udf_parse_xml'
packages = ('lxml')
as
$$
def udf_parse_xml(content: str):
    
    import lxml.etree as etree
    res = {}
    root = etree.fromstring(content)
    ns = {'f': 'http://www.fpml.org/FpML-5/reporting'}

    paths = {
        'trade_date': '/f:eventActivityReport/f:header/f:creationTimestamp',
        'murex_trade_id': '/f:eventActivityReport/f:trade/f:tradeHeader/f:partyTradeIdentifier/f:tradeId[@tradeIdScheme = "http://www.lchclearnet.com/clearlink/coding-scheme/murex-trade-id"]',
        'usinamespace': '/f:eventActivityReport/f:trade/f:tradeHeader/f:partyTradeIdentifier/f:issuer[@issuerIdScheme = "USINamespace"]',
        'usi': '/f:eventActivityReport/f:trade/f:tradeHeader/f:partyTradeIdentifier/f:tradeId[@tradeIdScheme = "http://www.fpml.org/coding-scheme/external/unique-transaction-identifier"]',
    }

    for k, v in paths.items():
        res[k] = root.xpath(v, namespaces=ns)[0].text
            
    for item in root.xpath('/f:eventActivityReport/f:party', namespaces=ns):
        res[item.attrib['id']] = item.getchildren()[0].text

    return res  
$$;


-- Build a pipeline with Streams & Tasks
-- Create a target table
create or replace table trades.transformed.trade_message (message variant);

-- Create a stream on the raw table
create or replace stream trades.raw.trades_stream_cdc on table trades.raw.trades_stream show_initial_rows = TRUE;

-- Create a task running every minute to process new rows into the target table
create or replace task trades.raw.parse_trade_message_data_pipeline_task
    warehouse = 'HOL_WH'
    schedule = '1 minute'
WHEN
    SYSTEM$STREAM_HAS_DATA('TRADES_STREAM_CDC')
as 
    insert into trades.transformed.trade_message (message)
    select trades.public.udf_parse_xml(trade_msg) as message
    from trades.raw.trades_stream_cdc;

-- RESUME the task
ALTER TASK trades.raw.parse_trade_message_data_pipeline_task RESUME;

-- Rows should still be continuing to stream into our raw table
select count(*) from trades.raw.trades_stream;

-- Monitor rows being populated into the target table
-- As the task executes each minute
select count(*) from trades.transformed.trade_message;

-- Task execution can be monitored through the Snowsight UI
-- or via a query:
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'parse_trade_message_data_pipeline_task'))
    order by scheduled_time desc;
    
-- The transformed data can be seen in the target table
select * from trades.transformed.trade_message
order by message:trade_date desc;


-- Option to use Dynamic Tables to simplify pipelines
-- e.g. Create a Dynamic Table with a target lag of 1 minute
-- and use dot-notation to break values out into separate columns
create or replace dynamic table trades.transformed.trade_message_DT
target_lag = '1 minute'
warehouse = HOL_WH
as
select message:clearingBroker1::string as clearingBroker1,
       message:clearingDCO::string as clearingDCO,
       message:murex_trade_id::string as murex_trade_id,
       message:party1::string as party1,
       message:party2::string as party2,
       message:tradeSource::string as tradeSource,
       message:trade_date::string as trade_date,
       message:usi::string as usi,
       message:usinamespace::string as usinamespace
from trades.transformed.trade_message
;

-- You can manually refresh the Dynamic Table, or wait for the automatic refresh based on the target lag
alter dynamic table trades.transformed.trade_message_DT refresh;

-- Again, let's review the rows being populated into the target Dynamic Table
select count(*) from trades.transformed.trade_message_DT;

select * from trades.transformed.trade_message_DT order by trade_date desc;

-- Now let's look at exporting XML from a Snowflake table
-- Create a stage to contain the exported XML file(s)
USE SCHEMA transformed;

create or replace stage XML_DATA_OUT 
FILE_FORMAT = (TYPE = XML);

-- Simple example of a Python Stored Procedure
-- which will output to our stage
CREATE OR REPLACE PROCEDURE write_xml()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'lxml')
HANDLER = 'write_xml'
AS
$$
import pandas
import lxml

def write_xml(session) -> str: 
    tableName = 'trades.transformed.trade_message_DT'
    df = session.table(tableName).to_pandas()
    df = df.set_index('MUREX_TRADE_ID')
    df.to_xml(elem_cols=[ # or attr_cols to flatten
              'CLEARINGDCO', 'PARTY1', 'PARTY2', 'TRADESOURCE', 'TRADE_DATE', 'USI', 'USINAMESPACE',
              ], path_or_buffer='/tmp/test.xml')
    session.file.put('/tmp/test.xml', '@XML_DATA_OUT', auto_compress=False, overwrite=True)
    return 'Done'
$$;

-- Call the Stored Procedure
CALL write_xml();

-- View the file which is output to the stage
list @XML_DATA_OUT;

-- The content of the file can be queried directly from the stage
select * from @XML_DATA_OUT/test.xml;

-- Or SnowSQL can be used to download from the stage to a local machine
-- e.g. 
-- GET @xml_data_out/test.xml file:///tmp/data/;

