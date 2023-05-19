#!/usr/bin/env python

#import
from confluent_kafka import Consumer, KafkaError, KafkaException
import time
import json
import argparse
import socket
import sys

def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = msg.value()
    dval = json.loads(val)
    print(time_start, dval)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str, help='Name of Kafka topic to stream')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'group.id': socket.gethostname()}
    
    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe([args.topic])

            msg=consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    except KeyboardInterrupt:
        pass
    
    finally:
        #close down consumer to comit final offsets
        consumer.close()

if __name__ == "__main__":
    main()