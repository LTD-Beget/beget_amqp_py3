# -*- coding: utf-8 -*-

import pika
import traceback

from pika.exceptions import ChannelClosed, ConnectionClosed

from .helpers.logger import Logger


class AmqpListen:
    """
    Класс подключения к AMQP, прослушки очереди и передачи сообщений в указанную функцию
    """

    WORK_RUNNING = 1
    WORK_STOPPING = 2

    def __init__(self,
                 host,
                 user,
                 password,
                 virtual_host,
                 queue,
                 callback,
                 consumer_storage,
                 port=5672,
                 durable=True,
                 auto_delete=True,
                 no_ack=False,
                 prefetch_count=1,
                 inactivity_timeout=60):

        self.logger = Logger.get_logger()

        self.host = host
        self.user = user
        self.password = password
        self.virtual_host = virtual_host
        self.queue = queue
        self.callback = callback
        self.port = port

        self.durable = durable
        self.auto_delete = auto_delete
        self.no_ack = no_ack
        self.prefetch_count = prefetch_count
        self.inactivity_timeout = inactivity_timeout

        self.consumer_storage = consumer_storage

        self.work_status = self.WORK_RUNNING

        # Обнуляем
        self.connection = None
        self.channel = None

    def start(self):
        """
        Начать прослушку и передачу сообщения в callback
        (При вызове, программа попадает в цикл. Выход рекомендован по сигналам)
        """
        self.logger.debug('AmqpListen: start listen:\n'
                          '  host: %s\n'
                          '  port: %s\n'
                          '  VH: %s\n'
                          '  queue: %s\n'
                          '  prefetch_count: %s\n'
                          '  user: %s\n'
                          '  pass: %s', self.host, self.port, self.virtual_host, self.queue, self.prefetch_count,
                          self.user, self.password)

        credentials = pika.PlainCredentials(self.user, self.password)
        connect_params = pika.ConnectionParameters(self.host, self.port, self.virtual_host, credentials)

        try:
            self.connection = pika.BlockingConnection(connect_params)
        except ConnectionClosed:
            self.logger.debug("AmqpListen: unable to connect to rabbit, aborting")
            return

        self.channel = self.connection.channel()
        """:type : BlockingChannel"""

        try:
            self.channel.queue_declare(queue=self.queue, passive=True)
        except ChannelClosed:
            self.logger.debug('AmqpListen: queue is not created, creating it')
            self.channel = self.connection.channel()
            """:type : BlockingChannel"""
            self.channel.queue_declare(queue=self.queue, durable=self.durable, auto_delete=self.auto_delete)

        self.channel.basic_qos(prefetch_count=1)

        while self.work_status == self.WORK_RUNNING:
            try:
                self.connection.sleep(0.1)
            except ConnectionClosed:
                self.logger.debug("AmqpListen: connection is closed, aborting")
                self.work_status = self.WORK_STOPPING
                continue
            except IOError:
                self.logger.debug('AmqpListen: syscall interrupt?')
                continue

            if not self.consumer_storage.consumer_is_allowed():
                continue

            try:
                channel_data = next(self.channel.consume(queue=self.queue, no_ack=self.no_ack), None)
                self.channel.cancel()
            except ConnectionClosed:
                self.logger.debug('AmqpListen: connection is closed, aborting')
                self.work_status = self.WORK_STOPPING
                continue

            if channel_data is None:
                self.logger.debug('AmqpListen: nothing to consume, continue')
                continue

            method_frame, properties, body = channel_data

            try:
                self.callback(self.channel, method_frame, properties, body)
            except ConnectionClosed:
                self.logger.debug('AmqpListen: connection is closed, aborting')
                self.work_status = self.WORK_STOPPING
                continue

    def stop(self):
        """
        Завершить прослушивание
        """
        self.logger.debug('AmqpListen: stop listen')
        self.work_status = self.WORK_STOPPING
