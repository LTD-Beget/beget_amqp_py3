# -*- coding: utf-8 -*-

from multiprocessing import Process

import filelock

from .consumer.storage.consumer_storage_redis import ConsumerStorageRedis
from .dependence.storage.dependence_storage_redis import DependenceStorageRedis
from .message_constructor import MessageConstructor
from .listen import AmqpListen
from .helpers.logger import Logger, uid_logger_wrapper_method, LoggerAdapterRequestId
from .message.storage.message_storage_redis import MessageStorageRedis
from ..Callbacker import Callbacker

from .exception.CallbackData import CallbackData

import signal
import os
import time
import traceback

import setproctitle


class AmqpWorker(Process):

    WORKING_YES = True  # Воркер занимается выполнением задачи
    WORKING_NOT = False  # Воркер не выполняет задач

    STATUS_START = True  # Воркер продолжает работу
    STATUS_STOP = False  # Воркер завершает работу

    MESSAGE_DONE_NOT = '0'  # Сообщение было отработано
    MESSAGE_DONE_YES = '1'  # Сообщение еще не отработано

    LOCAL_STORAGE_LIVE_TIME = 60 * 60 * 24 * 2  # Время хранения информации в локальном хранилище

    def __init__(self,
                 host,
                 user,
                 password,
                 virtual_host,
                 queue,
                 handler,
                 sync_manager,
                 port=5672,
                 durable=True,
                 auto_delete=False,
                 no_ack=False,
                 prefetch_count=1,
                 uid='',
                 sender=None,
                 max_la=0,
                 redis_host='localhost',
                 redis_port=6379):
        Process.__init__(self)

        self._name = self.name
        self.logger = Logger.get_logger()
        self.host = host
        self.user = user
        self.password = password
        self.virtual_host = virtual_host
        self.queue = queue
        self.handler = handler
        self.sync_manager = sync_manager
        """:type : beget_amqp.lib.dependence.sync_manager.SyncManager"""
        self.port = port
        self.durable = durable
        self.auto_delete = auto_delete
        self.no_ack = no_ack
        self.prefetch_count = prefetch_count
        self.uid = uid
        self.sender = sender
        self.max_la = max_la
        self.redis_host = redis_host
        self.redis_port = redis_port

        self.consumer_storage = ConsumerStorageRedis(worker_id=self.uid, amqp_vhost=virtual_host, amqp_queue=queue,
                                                     redis_host=self.redis_host, redis_port=self.redis_port)
        self.message_storage = MessageStorageRedis(worker_id=self.uid, amqp_vhost=virtual_host, amqp_queue=queue,
                                                   redis_host=self.redis_host, redis_port=self.redis_port)
        self.dependence_storage = DependenceStorageRedis(worker_id=self.uid, amqp_vhost=virtual_host, amqp_queue=queue,
                                                         redis_host=self.redis_host, redis_port=self.redis_port)

        # обнуляем
        self.amqp_listener = None
        self.current_message = None  # Для хранения обрабатываемого сообщения
        self.working_status = self.WORKING_NOT  # Получили и работаем над сообщением?
        self.program_status = self.STATUS_START  # Программа должна выполняться и дальше? (Для плавного выхода)

        # create worker lock
        self.worker_lock = filelock.FileLock(AmqpWorker.get_worker_lockfile(self.uid))

        self.logger.debug("Created worker {} for queue {}".format(self.uid, self.queue))

    @staticmethod
    def get_worker_lockfile(worker_id):
        # avoid circular imports
        from .. import get_lockfile
        return get_lockfile('worker:{}'.format(worker_id))

    @staticmethod
    def is_worker_alive(worker_id):
        worker_lock = filelock.FileLock(AmqpWorker.get_worker_lockfile(worker_id))

        try:
            worker_lock.acquire(timeout=0.1)
            return False
        except filelock.Timeout:
            return True

    def run(self):
        """
        Начинаем работать в качестве отдельного процесса.
        """
        # Изменяем имя процесса для мониторинга
        process_title = setproctitle.getproctitle()
        process_title += '_' + self._name
        setproctitle.setproctitle(process_title)

        self._name += '({})'.format(os.getpid())
        self._name += '[{}]'.format(self.uid[:8])

        # Назначаем сигналы для выхода
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGHUP, self.sig_handler)

        self.debug('Started worker {}'.format(self.uid))

        # hold mutex until we die
        self.worker_lock.acquire()

        # Начинаем слушать AMQP и выполнять задачи полученные из сообщений:
        try:
            self.amqp_listener = AmqpListen(self.host,
                                            self.user,
                                            self.password,
                                            self.virtual_host,
                                            self.queue,
                                            self._on_message,
                                            self.consumer_storage,
                                            self.port,
                                            self.durable,
                                            self.auto_delete,
                                            self.no_ack,
                                            self.prefetch_count)
            self.amqp_listener.start()
        except Exception as e:
            self.error('Exception: %s\n'
                       '  %s\n', e, traceback.format_exc())

        self.debug('Correct exit from multiprocessing')

    @uid_logger_wrapper_method
    def _on_message(self, channel, method, properties, body):
        """
        Обрабатываем сообщение полученное из AMQP

        :param channel:  канал подключения.
        :type channel: pika.adapters.blocking_connection.BlockingChannel

        :param method:  метод
        :type method: pika.spec.Deliver

        :param properties: параметры сообщения
        :type properties: pika.spec.BasicProperties

        :param body: тело сообщения
        :type body: basestring
        """
        self.debug('get-message: properties={}, method={}, body={}'.format(properties, method, body))

        self.check_allowed_to_live()

        # Получаем объект сообщения из сырого body
        message_constructor = MessageConstructor()
        message_amqp = message_constructor.create_message_amqp(body, properties)
        message_to_service = message_constructor.create_message_to_service_by_message_amqp(message_amqp)

        LoggerAdapterRequestId.static_global_request_id = message_amqp.global_request_id

        # Проверяем в локальном хранилище, что это не дублирующая заявка
        if self.message_storage.is_duplicate_message(message_amqp):
            if self.message_storage.is_done_message(message_amqp):
                self.consumer_storage.consumer_release()
                self.sync_manager.remove_unacknowledged_message_id(message_amqp.id)
                if not self.no_ack:
                    self.debug('Acknowledge delivery_tag: %s', method.delivery_tag)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            else:
                worker_id_alive_list = self.sync_manager.get_workers_id()
                worker_id = self.message_storage.get_worker_id_by_message(message_amqp)
                if worker_id in worker_id_alive_list:
                    # Todo: Rabbit don't allow get custom or another message.
                    # Todo: Exclude the receipt of this message for this channel
                    self.consumer_storage.consumer_release()
                    self.sync_manager.add_unacknowledged_message_id(message_amqp.id)
                    time.sleep(10)
                    if not self.no_ack:
                        self.debug('No acknowledge delivery_tag: %s', method.delivery_tag)
                        channel.basic_nack(delivery_tag=method.delivery_tag)
                    return

        # Сохраняем информацию о заявке в локальное хранилище
        self.message_storage.message_save(message_amqp, body, properties)
        self.sync_manager.set_message_on_work(message_amqp)

        # Устанавливаем зависимости сообщения
        self.set_dependence(message_amqp)
        self.debug('set-dependence: properties={}, method={}, body={}'.format(properties, method, body))

        self.consumer_storage.consumer_release()

        try:
            self.debug('Wait until dependence {} to be free'.format(message_amqp.dependence))
            self.wait_dependence(message_amqp)
            self.debug('Dependence {} is ready to execute callback'.format(message_amqp.dependence))

            self.working_status = self.WORKING_YES
            # Ждем, пока Load Average на сервере будет меньше чем задан в настройках
            if self.max_la > 0:
                self.wait_load_average()

            self.message_storage.message_save_start_time(message_amqp)

            if not self.is_ttl_expired(message_amqp):
                # Основная строчка кода, всего пакета:
                callback_result = self.handler.on_message(message_to_service)
            else:
                callback_result = self.handler.on_message_expired(message_to_service)

            Callbacker.send(self.sender, Callbacker.EVENT_SUCCESS, message_amqp, callback_result)

        except CallbackData as e:
            try:
                Callbacker.send(self.sender, e.callback_key, message_amqp, e.data)
            except Exception as e:
                self.error('Exception while send callback: %s\n  %s\n', str(e), traceback.format_exc())

        except Exception as e:
            # При возникновение ошибки, используем стандартизированный формат сообщения:
            callback_result = {
                'error': {
                    # Сообщение - Первый аргумент исключения, если это строка. Иначе, берется __str__
                    'message': e.args[0] if len(e.args) and isinstance(e.args[0], str) else str(e),

                    # Код - берется поле code, иначе 1
                    'code': e.code if hasattr(e, 'code') else 1,
                    'trace': e.trace if hasattr(e, 'trace') else traceback.format_exc()
                }
            }
            try:
                self.error('Exception from Handler: %s\n  %s\n',
                           callback_result['error']['message'],
                           callback_result['error']['trace'])
                Callbacker.send(self.sender, Callbacker.EVENT_FAILURE, message_amqp, callback_result)
            except Exception as e:
                self.error('Exception while send callback: %s\n  %s\n', e, traceback.format_exc())

        self.sync_manager.set_message_on_work_done(message_amqp)
        self.message_storage.message_set_done(message_amqp)
        self.release_dependence(message_amqp)
        if not self.no_ack:
            self.debug('Acknowledge delivery_tag: %s', method.delivery_tag)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        self.working_status = self.WORKING_NOT

        # Если за время работы над сообщением мы получили команду выхода, то выходим
        self.check_allowed_to_live()

    def wait_load_average(self):
        """
        Зависает в цикле если load average больше допустимого, выходит как нагрузка стабилизируется
        :return:
        """
        while True:
            la = os.getloadavg()[0]
            if la > self.max_la:
                self.debug("Load average too high, current: {0}, limit: {1}, sleeping".format(la, self.max_la))
                time.sleep(5)
            else:
                self.debug("Load average fine, current: {0}, limit: {1}, proceeding".format(la, self.max_la))
                break

    def set_dependence(self, message):
        """
        Ставим зависимость сообщения в очередь.
        :type message: MessageAmqp
        """
        if not message.dependence:
            return

        self.debug('set-dependence: {}'.format(message.dependence))
        self.dependence_storage.dependence_set(message)

    def wait_dependence(self, message):
        """
        Ожидаем пока зависимость освободится
        :type message: MessageAmqp
        """
        if not message.dependence:
            return

        self.debug('wait-dependence: {}'.format(message.dependence))
        while True:
            if self.dependence_storage.dependence_is_available(message):
                return True
            time.sleep(0.1)

    def release_dependence(self, message):
        """
        Освобождаем зависимость
        :type message: MessageAmqp
        """
        if not message.dependence:
            return

        self.debug('release-dependence: {}'.format(message.dependence))
        self.dependence_storage.dependence_release(message)

    def sig_handler(self, sig_num, frame):
        """
        Обработчик сигналов
        """
        self.debug('get signal %s', sig_num)
        if sig_num is signal.SIGHUP or sig_num is signal.SIGTERM:
            self.stop()

    ################################################################################
    # Функции обработки аварийных ситуация и выхода

    def check_allowed_to_live(self):
        """
        Проверяем разрешение на продолжение работы и обработываем ситуацию аварийного выхода
        """
        if self.program_status is self.STATUS_STOP:
            self.stop()

        if not self.is_main_process_alive():
            self.handler_error_main_process()

        if not self.is_sync_manager_alive():
            self.handler_error_sync_manager()

        return True

    def is_main_process_alive(self):
        """
        Жив ли основной процесс
        """
        if os.getppid() == 1:
            return False
        return True

    def is_sync_manager_alive(self):
        """
        Жив ли SyncManager
        """
        try:
            self.sync_manager.check_status()
            return True
        except:
            return False

    def handler_error_sync_manager(self):
        """
        Обработчик ситуации, когда SyncManager мертв
        """
        self.critical('SyncManager is dead, but i\'m alive. Program quit')
        if self.is_main_process_alive():
            os.kill(os.getppid(), signal.SIGHUP)
        self.stop()

    def handler_error_main_process(self):
        """
        Обработчик ситуации, когда основной процесс мертв
        """
        self.critical('Main process is dead, but i\'m alive. Program quit')
        try:
            self.sync_manager.stop()
        except:
            pass
        self.stop()

    def stop(self):
        """
        Корректное завершение
        """
        if self.working_status is self.WORKING_NOT:
            self.debug('immediately exit')
            os.kill(os.getpid(), 9)  # todo корректный выход
            # self.amqp_listener.stop()
        else:
            self.debug('stop when the work will be done')
            self.program_status = self.STATUS_STOP

    def is_ttl_expired(self, message_amqp):
        """
        Превысило ли сообщение время ожидания
        :type message_amqp: MessageAmqp
        :rtype : bool
        """
        if message_amqp.expiration == 0:
            return False

        if message_amqp.expiration < time.time():
            self.info('Message expired: %s', message_amqp.id)
            return True

        return False

    ################################################################################
    # Логирование

    def debug(self, msg, *args):
        self.logger.debug('%s: ' + msg, self._name, *args)

    def info(self, msg, *args):
        self.logger.info('%s: ' + msg, self._name, *args)

    def critical(self, msg, *args):
        self.logger.critical('%s: ' + msg, self._name, *args)

    def error(self, msg, *args):
        self.logger.error('%s: ' + msg, self._name, *args)
