import json
import time
from kafka import KafkaProducer
from random import normalvariate, randint, choices, choice

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def read_config(filename='./kafka_producer_config.json'):
    with open(filename, 'r') as f:
        config = json.loads(f.read())
    return config

def createProducer(config={}):
    brokers_down = True
    while brokers_down:
        try:
            p = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), **config)
            brokers_down = False
        except:
            pass
    return p

def generate_random_message(timestamp=None):
    if timestamp is None:
        timestamp = time.time()

    # Read the base xml file to use for our messages
    with open('LCH00012345678_NEW_TRADE.xml') as file:
        base_xml = file.read()
    msg_xml = base_xml.replace('12345678', str(randint(10000000,99999999)))
    trade_msg = msg_xml
    msg = {
            "trade_msg": trade_msg, 
            "timestamp": timestamp
            }
    return msg

def send_message(producer, topic_config):
    msg_timestamp = None
    if topic_config['messageDate'] is not None:
        msg_timestamp = topic_config['messageDate']
        topic_config['messageDate'] += topic_config['messageFrequency']
    else:
        time.sleep(topic_config['messageFrequency'])
    msg = generate_random_message(timestamp=msg_timestamp)
    print('Generating a message at {}'.format(time.time()))
    producer.send(topic_config['topic'], value=msg)
    return topic_config

def main():
    producer_config = read_config()
    topic_config = read_config('./kafka_topic_config.json')
    logger.info('Kafka Producer config: {}'.format(producer_config))
    logger.info('Kafka Topic config: {}'.format(topic_config))
    snowpipe_producer = createProducer(config=producer_config)
    logger.info('Successfully created Producer')

    messageCount = 0
    if topic_config['numMessages'] is not None:
        while messageCount < topic_config['numMessages']:
            try:
                topic_config = send_message(snowpipe_producer, topic_config)
                messageCount += 1
            except KeyboardInterrupt:
                break
            if (messageCount % 100000) == 0:
                logger.info('Generated {} msgs'.format(messageCount))
    else:
        while True:
            try:
                topic_config = send_message(snowpipe_producer, topic_config)
                messageCount += 1
                if topic_config['messageDate'] is not None:
                    if topic_config['messageDate'] >= time.time():
                        topic_config['messageDate'] = None
            except KeyboardInterrupt:
                print("\nExiting due to user Ctrl+C\n")
                break
            if (messageCount % 100000) == 0:
                logger.info('Generated {} msgs'.format(messageCount))



if __name__ == "__main__":
    main()