-- Snowpipe Streaming / XML hands-on lab setup
-- Create database and schema to stream data into
-- grant required role, privileges etc.  
-- create a user for Snowpipe Streaming to use, configure RSA public key, and grant role to it.

use role SYSADMIN;
CREATE OR REPLACE WAREHOUSE hol_wh WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'ECONOMY';

create database if not exists trades;
use database trades;
create schema if not exists raw;
use schema raw;

use role securityadmin;

-- -- Create a Snowflake role with the privileges to work with the connector.
create or replace role kafka_connector_role_1;

-- -- Grant privileges on the database.
grant usage on database trades to role kafka_connector_role_1;

-- -- Grant privileges on the schema and tables
grant usage on schema trades.raw to role kafka_connector_role_1;
grant create table on schema trades.raw to role kafka_connector_role_1;
grant create stage on schema trades.raw to role kafka_connector_role_1;
grant create pipe on schema trades.raw to role kafka_connector_role_1;
grant all on all tables in schema trades.raw to role kafka_connector_role_1;
grant all on future tables in schema trades.raw to role kafka_connector_role_1;

-- -- Grant the custom role to our SYSADMIN role
grant role kafka_connector_role_1 to role SYSADMIN;

-- Please refer to the section in readme.md covering how to generate RSA Keys for the keypair authentication which the Kafka connector will use
-- Create a user specifically for the kafka connector
-- and set its RSA public Key to use keypair authentication
-- Note that the public key should be entered as a single string with no line breaks
CREATE USER kafka_user PASSWORD='!abc123!' DEFAULT_ROLE = PUBLIC MUST_CHANGE_PASSWORD = FALSE;
alter user kafka_user set rsa_public_key='<enter public key here';
DESCRIBE USER kafka_user;

-- -- Grant the custom role to our newly created kafka_user user
grant role kafka_connector_role_1 to user kafka_user;

-- -- Set the custom role as the default role for the user that the KC is using:
-- -- If you encounter an 'Insufficient privileges' error, verify the role that has the OWNERSHIP privilege on the user.
alter user kafka_user set default_role = kafka_connector_role_1;

-- Cleanup :
/*
use role ACCOUNTADMIN;
DROP DATABASE TRADES;
DROP USER kafka_user;
DROP role kafka_connector_role_1;
drop warehouse hol_wh;
*/



