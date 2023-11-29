# Snowpipe Streaming Ingest
## Lab / Demo

This repo contains the necessary code, configurations, and SQL files to complete a Lab / Demo around Snowpipe Streaming Ingest. It consists of three main components:
1. a Python client app to stream synthetically generated trades (in the form of XMl/FPML messages) into an Apache Kafka topic
2. the Snowflake Kafka Connector (KC) configurations necessary to ingest these ad records into a Snowflake table using Snowpipe Streaming
3. the SQL files necessary to both configure the permissions/roles in the Snowflake account to conduct the lab / demo, as well as build downstream 


### Requirements

#### For Docker (Preferred Method)
- Docker desktop **(please ensure that you have logged in to Docker Desktop, otherwise subsequent Docker commands may fail)**
- A Snowflake account with key authentication configured for the relevant user role

#### For Local Runtime (Not Preferred)
- a Conda Python environment
- open-source Apache Kafka 2.13-3.1.0 installed locally
- Snowflake Kafka Connector 1.8.1 jar
- openJDK <= 15.0.2 (there is an `arrow` bug introduced for JDK 16+ that will cause errors in the Kafka Connector, so the machine must be equipped with openjDK <=15.0.2)

We will go through the steps of installing and running Apache Kafka, configuring and launching the Kafka Connector, configuring and launching the Python streaming client, configuring your Snowflake account, and running the demo.

### Snowflake - Account Set Up
#### Account Configuration

- Clone this repository to your local machine that has Docker Desktop installed: `git clone git@github.com:sfc-gh-jhunt/streaming_trades_xml_snowpark.git`

Open up the `./setup_kafka_connector.sql` file into a Snowsight worksheet and run the SQL commands to create databases, schemas, etc. and configure the `kafk_connector_role_1` role that KC is going to use to ingest data using Snowpipe Streaming

#### Configure RSA Keys for the KCs Account
- Create keys (for the lab/demo we will use unencrypted keys) via the instructions located [here](https://docs.snowflake.com/en/user-guide/kafka-connector-install.html#using-key-pair-authentication-key-rotation). We recommend naming the keys `sf_kafka_rsa_key.pem` and `sf_kafka_rsa_pub_key.pub` and putting them in the `./include/` directory. These will be ignored by Git.
- Note that Windows PCs/Laptops may not have openssl installed by default. Links to download Windows binaries are available at https://wiki.openssl.org/index.php/Binaries 
- Add public key to user in Snowflake:
```sql
alter user <USERNAME> set rsa_public_key='copy_public_key_here';
```

### Docker Setup - Preferred Method
We have provided a method for you to build and run all of the necessary components for this demo via a single `docker-compose.yml`. This will launch four services: Zookeeper, Apache Kafka, Kafka Connect, and the Python Streaming Client. To get started with this demo via Docker:

- Open up the `SF_connect.properties` file in your text editor of choice and modify the following properties with your appropriate demo account information: `snowflake.url.name`, `snowflake.user.name`, and `snowflake.private.key`. If you are using an encrypted private key, you'll also neeed to uncomment and include the `snowflake.private.key.passphrase`, however if your key is not encrypted, leave this line commented out. Note that the private key should be entered as a single string with no line breaks. Storing plaintext credentials 

- Once you have modified and saved the properties, you can build and launch the project:

```zsh
cd /path/to/repo/
docker-compose build
docker-compose up
```
This `docker-compose up` command will launch:
1. a Zookeeper instance
2. a local Kafka cluster
3. a Kafka connect instance with the Snowflake connector configured to ingest data into Snowflake
4. a Python client to stream fake data to Kafka

### Snowflake - Viewing & Querying Data in the Demo
#### Options to create transformed table / Dynamic Table
Load the `./lab_walkthrough.sql` file into a Snowsight worksheet (or your IDE of choice) and walk through the SQL commands to query and transform the data.

---

### Local Setup - Not Preferred
### Apache Kafka `2.13-3.3.1`
#### Installation
Follow step 1 in the [Apache Kafka Quickstart](https://kafka.apache.org/quickstart) to download and install Kafka 2.13-3.3.1. You can also launch Kafka and run steps 3-5 to ensure Kafka is working properly.

We recommend downloading and extracting Kafka into a directory such as `~/dev` or `~/apache_kafka`. We will refer to this download location (e.g. `~/dev/kafka_2.13-3.1.0/`) as `<kafka_dir>` throughout this README.
#### Running Kafka
Navigate to `<kafka_dir>` and launch zookeeper:
```zsh
cd <kafka_dir>
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Open up another terminal window. Navigate to `<kafka_dir>` and launch the Kafka broker service:
```zsh
$ bin/kafka-server-start.sh config/server.properties
```
Now, Kafka should be up and running. You can also run steps 3-5 in the quickstart linked above to verify that the Kafka broker is working properly.

### Kafka Connector `1.8.1`
#### Installation
Refer to the [documentation](https://docs.snowflake.com/en/user-guide/kafka-connector-install.html#download-the-kafka-connector-jar-files) for full instructions, but the general instructions are as follows:
1. Download the [`Snowflake Kafka Connector 1.8.1 JAR`](https://mvnrepository.com/artifact/com.snowflake/snowflake-kafka-connector/1.8.1) from Maven
2. Copy the JAR into `<kafka_dir>/libs`

That's it!

#### Configure RSA Keys for the KCs Account
- Create keys (for the demo we will use unencrypted keys) via the instructions located [here](https://docs.snowflake.com/en/user-guide/kafka-connector-install.html#using-key-pair-authentication-key-rotation). We recommend naming the keys `sf_kafka_rsa_key.pem` and `sf_kafka_rsa_pub_key.pub` and putting them in the `./include/` directory. These will be ignored by Git.
- Add public key to user in Snowflake:
```sql
alter user <USERNAME> set rsa_public_key='copy_public_key_here';
```
- Add private key to [`./SF_connect.properties`](./SF_connect.properties) under `snowflake.private.key=`
- **If you are using an encrypted key** you'll also need to uncomment and set the `snowflake.private.key.passphrase=` propery. If your private key is not encrypted, you must leave this property commented out.

#### Configure the KC
Modify [`./SF_connect.properties`](./SF_connect.properties) file to reflect the desired demo account information. Likely changes include:
- `snowflake.url.name`
- `snowflake.user.name`
- `snowflake.private.key`
- `snowflake.private.key.passphrase` if you are using encrypted keys

```properties
name=snowpipe_streaming_ingest
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=8
topics=digital_ad_records
snowflake.topic2table.map=digital_ad_records:streaming_ad_record
buffer.count.records=10000
buffer.flush.time=10
buffer.size.bytes=20000000
snowflake.url.name=ACCOUNT_LOCATOR.snowflakecomputing.com:443
snowflake.user.name=USER_NAME_HERE
snowflake.private.key=YOUR_KEY_HERE
# snowflake.private.key.passphrase=YOUR_PASSPHRASE_IF_USING_ENCRYPTED_KEY_HERE
snowflake.database.name=snowpipe_streaming
snowflake.schema.name=dev
key.converter=org.apache.kafka.connect.storage.StringConverter
# value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
snowflake.role.name=kafka_connector_role_1
snowflake.ingestion.method=SNOWPIPE_STREAMING
```

Ensure that `snowflake.ingestion.method` is set to `SNOWPIPE_STREAMING` and that `key.converter` and `value.converter` are both set to `org.apache.kafka.connect.storage.StringConverter`

#### Running KC
Once your [`./SF_connect.properties`](./SF_connect.properties) file has all of the correct info, copy and paste the file into `<kafk_dir>/config`. Then, assuming that your Snowflake account has been properly enabled, you'll be able to launch the Kafka Connector via:
```zsh
<kafka_dir>/bin/connect-standalone.sh <kafka_dir>/config/connect-standalone.properties <kafka_dir>/config/SF_connect.properties
```

### Python Streaming Client
#### Environment
We are assuming that there is an existing conda virtual environment being built for the other components of the demo. The Python streaming client can run in this same virtual environment. To install any additional dependencies:
```zsh
# activate the conda environment
conda activate <env_name>
pip install -r ./requirements.txt
```
#### Configuration
There are two relevant configuration files:
1. [`./kafka_producer_config.json`](./kafka_producer_config.json) which should not need to be modified because we are running a standalone local Kafka instance
2. [`./kafka_topic_config.json`](./kafka_topic_config.json) which should ONLY be modified if you want to either (1) change the streaming message frequency or (2) backfill historical data.

To change the streaming message frequency, change the `messageFrequency` param. This param control the amount of time (in seconds) between subsequent messages being sent from the Python client to a Kafka topic. E.g. `"messageFrequency": 5` implies a message will be sent every 5 seconds, while `"messageFrequency": 0.01` means 100 msgs/sec will be written to Kafka.

To backfill historical data, you can set the `messageDate` param to the unix timestamp that you want to correspond to the date/time of the *first* record produced. So, to backfill data beginning May 1, 2012 at 12:00:01am, set `"messageDate": 1335852060`. The client will then generate messages *as fast as it can* and publish them to Kafka, producing timestamps in the data records based on the message's relation to the `messageDate` configured value. There is an example of this in the [`./kafka_topic_config_for_historical_backfill.json`](./kafka_topic_config_for_historical_backfill.json) sample file (**THIS SAMPLE FILE IS NOT USED AT RUNTIME, ONLY THERE FOR DEMONSTRATION PURPOSES**).

So, as an example, if you have `"messageFrequency": 5, "messageDate": 1335852060`, then the second message will be produced with a datetime of `1335852065`, five seconds later. This is not reflected by the *wall clock* however- the messages are produced and published as fast as the client can possibly produce them. Once the program's clock reaches the current wall-clock, the program will fall back into a real-time operating mode and stream data normally, in sync with the real-world wall clock. This provides a handy way to initialize the demo environment database with 10+ years of historical data. It will take several minutes for that much data to stream and populate, but is simpler than otherwise generating CSVs, bulk loading, etc.

If you do not need to backfill historical data, leave the `messageDate` param set to `null`

#### Running the Client
Once you've set the `./kafka_topic_config.json` params appropriately, run the client in a terminal window using your conda environment:
```zsh
conda activate <env_name>
python ./snowpipe_streaming_kafka_producer.py
```
The client will log output to the terminal every X messages, and you should see data populating in your Snowflake account from the Kafka Connector!

---
