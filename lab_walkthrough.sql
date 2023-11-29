
--- set context
use role sysadmin;
use warehouse hol_wh;
use database trades;
use schema raw;

-- Check the data is streaming in to our raw.trades table
select count(*) from trades_stream ;
select * from  trades_stream order by timestamp desc limit 100;

-- One option is to convert the whole XML schema to JSON and query using dot notation:
-- xmltodict package can be called in a udf to achieve this.
create or replace function xml2json(xml_string varchar)
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
select 
record_metadata,
trade_msg,
xml2json(trade_msg) as trade_msg_json
from trades.raw.trades_stream
limit 100;

-- Or used to populate a transformed table containing the JSON:
create table trades.transformed.trades_json as (
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


-- Or create a pipeline using a Dynamic Table 
create or replace dynamic table trades.transformed.trade_message_dt
target_lag = '1 minute'
warehouse = HOL_WH
as
select trades.public.udf_parse_xml(trade_msg):clearingDCO::string as clearing_dco,
       count(*) as number_of_trades
from trades.raw.trades_stream 
group by 1;
