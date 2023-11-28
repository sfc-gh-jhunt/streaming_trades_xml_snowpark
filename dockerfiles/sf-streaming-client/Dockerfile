FROM python:3.8-bullseye

WORKDIR /client/snowpipe_streaming

COPY requirements.txt /client/snowpipe_streaming/
RUN pip install -r requirements.txt

COPY snowpipe_streaming_kafka_producer.py /client/snowpipe_streaming/
COPY LCH00012345678_NEW_TRADE.xml /client/snowpipe_streaming/
COPY kafka_producer_config_docker.json /client/snowpipe_streaming/kafka_producer_config.json
COPY kafka_topic_config.json /client/snowpipe_streaming/

CMD ["python", "./snowpipe_streaming_kafka_producer.py"]