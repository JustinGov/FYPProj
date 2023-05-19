#!/usr/bin/env python

#import
import os, subprocess
from subprocess import check_output
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
    processRT(dval)

def processRT(dval):
    try:
        if dval:
            command5 = f'spark-submit cleanRTData.py data.csv'
            # Run the command and capture the output
            output = subprocess.check_output(command5, shell=True, stderr=subprocess.STDOUT)
            #Print the output
            print(output.decode())

        # Show a message box after the function has finished running
            #messagebox.showinfo('Finished', 'Processing complete!')
            print("Processing complete")
        else:
            raise ValueError("None streamed")
    except Exception as e:
        # Show a message box with the error message if an exception occurs
        #messagebox.showerror('Error', str(e))
        print("Error" + str(e))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str, help='Name of Kafka topic to stream')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'kafka:9092',
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
                #processRT(msg)
                msg_process(msg)
                
    except KeyboardInterrupt:
        pass
    
    finally:
        #close down consumer to comit final offsets
        consumer.close()

if __name__ == "__main__":
    main()