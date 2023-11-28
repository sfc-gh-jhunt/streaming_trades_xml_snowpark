alter user <MY-USER> set rsa_public_key='copy_public_key_here';
desc user <MY-USER>;

use role ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE kafka_wh WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'ECONOMY';

create database snowpipe_streaming;
use database snowpipe_streaming;
create schema dev;
use schema dev;

--- Only need below if you intend to not use ACCOUNTADMIN role
--- If you are going to use the kafka_connector_role_1, make sure you modify SF_connect.properties to set snowflake.role.name=kafka_connector_role_1
--- SF_connect.properties snowflake.role.name MUST match the user default role
-- -- Use a role that can create and manage roles and privileges.
use role securityadmin;

-- -- Create a Snowflake role with the privileges to work with the connector.
create role kafka_connector_role_1;

-- -- Grant privileges on the database.
grant usage on database snowpipe_streaming to role kafka_connector_role_1;

-- -- Grant privileges on the schema.
grant usage on schema dev to role kafka_connector_role_1;
grant create table on schema dev to role kafka_connector_role_1;
grant create dynamic table on schema dev to role kafka_connector_role_1;
grant create stage on schema dev to role kafka_connector_role_1;
grant create pipe on schema dev to role kafka_connector_role_1;

-- //-- Only required if the Kafka connector will load data into an existing table.
-- //grant ownership on table streaming_ad_record to role kafka_connector_role_1;


-- -- Grant the custom role to an existing user.
grant role kafka_connector_role_1 to user <MY-USER>;

-- -- Set the custom role as the default role for the user that the KC is using:
-- -- If you encounter an 'Insufficient privileges' error, verify the role that has the OWNERSHIP privilege on the user.
alter user <MY-USER> set default_role = kafka_connector_role_1;

grant usage on warehouse kafka_wh to role kafka_connector_role_1;

