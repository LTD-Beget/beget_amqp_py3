#!/usr/bin/env python

import os, sys
sys.path.insert(0, os.getcwd())

import pika
import json
import uuid

class Sender:
    def __init__(self):
        pass

    @staticmethod
    def send(url, queue, data):
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()

        try:
            channel.queue_declare(queue=queue, passive=True)
        except pika.exceptions.ChannelClosed:
            print('queue doesn\'t exist. Create new.')
            channel = connection.channel()
            channel.queue_declare(queue=queue, durable=True, auto_delete=True)

        channel.basic_publish('', queue, data, pika.BasicProperties(headers={'dependence': ['test']}))
        connection.close()

if __name__ == '__main__':
    import config as conf

    msg = {'controller': 'test', 'action': 'kill_me', 'params': {'sleep_time': 30}}

    Sender.send('amqp://' + conf.AMQP_USER +
                ':' + conf.AMQP_PASS +
                '@' + conf.AMQP_HOST +
                ':' + conf.AMQP_PORT +
                '/' + conf.AMQP_EXCHANGE, conf.AMQP_QUEUE, json.dumps(msg))

    print('send')
