
--- set context
use role sysadmin;
use warehouse hol_wh;
use database trades;
use schema raw;

-- Check the data is streaming in to our raw.trades table
select count(*) from trades_stream ;
select * from  trades_stream order by timestamp desc limit 100;

-- Example of how to convert to JSON first and query using dot notation?

-- Then move on to extraction from XML (Nick's content)

use schema public;

create or replace function udf_parse_xml(content varchar)
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
create or replace table trades.transformed.trade_message (message variant);

create or replace stream trades.raw.trades_stream_cdc on table trades.raw.trades_stream show_initial_rows = TRUE;

create or replace task trades.raw.parse_trade_message_data_pipeline_task
    warehouse = 'HOL_WH'
    scheduel = '1 minute'
as 
    insert into trades.transformed.trade_message (message)
    select trades.public.udf_parse_xml(trade_msg) as message
    from trades.raw.trades_stream_cdc;


-- Create a pipeline using a Dynamic Table 
create or replace dynamic table trades.transformed.trade_message_dt
target_lag = '1 minute'
warehouse = HOL_WH
as
select trades.public.udf_parse_xml(trade_msg):clearingDCO::string as clearing_dco,
       count(*) as number_of_trades
from trades.raw.trades_stream 
group by 1;
