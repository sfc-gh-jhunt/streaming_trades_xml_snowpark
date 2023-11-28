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

-- Create a user specifically for the kafka connector
-- and set its RSA public Key to use keypair authentication
CREATE USER kafka_user PASSWORD='!abc123!' DEFAULT_ROLE = myrole DEFAULT_SECONDARY_ROLES = ('ALL') MUST_CHANGE_PASSWORD = FALSE;
alter user kafka_user set rsa_public_key='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4e5XL31mgjHS3ADV42JpBD3k7Ac+2B6Al483BJi0fPkEEvtqhbWI+a+70Hh2vaMyelFns2VvNrQbD/KHaaCeBGzT/sLzFmEA8uwAh6NJ8ilAqqXtbwe69lFCDp/J0Hl+hwuOVNYytG+ObFuKz2qodyL+I8vAcswfYqgUBlcJMnWvWRnNZsX4Of/vBX5jJYqHjDcU+VSsNudH8t64vXMFn2t0T16koo7jQJ4Uhtjs5Uo/WuZFabNY74sYRmG7aNpOeTQTPzbd/AgS43WwyEwD9CliwUm7pCaIsLtv5hYHwXzRS2EMaHhO21Rg1MYOCctTpgNUHz5cJF6pfbJqwBBZvwIDAQAB';
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



