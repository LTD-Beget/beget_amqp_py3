# -*- coding: utf-8 -*-

import beget_amqp as amqp
import time
import os
import multiprocessing
import setproctitle


class TestController(amqp.Controller):
    """
    Пример контроллера
    """

    def action_empty(self):
        print('i`m empty')

    def action_print_type(self, some_arg):
        print('some_arg type:', type(some_arg))
        print('some_arg repr:', repr(some_arg))

    def action_print_any_argument(self, *args, **kwargs):
        print('args:', repr(args))
        print('kwargs:', repr(kwargs))

    def action_sleep(self, sleep_time=10, name=None):
        my_name = name or os.getpid()
        self.logger.debug('%s:start: sleep %s' % (my_name, str(sleep_time)))
        for i in range(1, sleep_time):
            time.sleep(1)
            self.logger.debug('%s:tick: %s [%s]' % (my_name, i, sleep_time))
        self.logger.debug('%s:exit' % my_name)

    def action_zombie(self, sleep_time=10, name=None):
        my_name = name or os.getpid()
        self.logger.debug('%s:start: sleep %s' % (my_name, str(sleep_time)))

        def worker(sleep_time):
            my_name = os.getpid()
            setproctitle.setproctitle('WORKER:{}'.format(my_name))
            self.logger.debug('WORKER:%s:start: sleep %s' % (my_name, str(sleep_time)))
            for i in range(1, sleep_time):
                time.sleep(1)
                self.logger.debug('WORKER:%s:tick: %s [%s]' % (my_name, i, sleep_time))
            self.logger.debug('WORKER:%s:exit' % my_name)

        p = multiprocessing.Process(target=worker, args=(sleep_time,))
        p.start()

        self.logger.debug('%s:exit' % my_name)

    #########################
    # Для проверки обратных вызовов:

    def action_success_callback(self, text):
        """
        Если в worker возвращается dict, то вызывается onSuccess callback. Ключи dict - аргументы принимающего action
        """
        return {'result': text + ' Success'}

    def action_read_success_callback(self, result):
        """
        Читаем ответ полученный из action_callback через callback
        """
        print('Read success:', repr(result))

    def action_error_callback(self):
        """
        Вызов типичной ошибки.
        Предполагается прокидываение ошибки до самого worker (возможно запакованной в другую ошибку)
        и вызов callback onFailure с передачей в нее сообщения, кода и traceback
        """
        raise Exception('Typical error in action', 99)

    def action_read_error_callback(self, error):
        """
        Чтение типичной ошибки из callback.
        Стандартный формат ошибки предполагает использование аргумента error который содержит массив с ключами:
        message, code, trace
        """
        print('Read error:', repr(error))

    def action_error_with_custom_data(self):
        """
        Похоже на вызов обычной ошибки с последующем вызовом onFailure callback, но в отличие от последней,
        нацелено на передачу данных кастомного формата в onFailure.
        """
        msg = {
            'status': 'error',
            'answer': 42,
            'accounts': [
                'fr3434e',
                'fr3454r'
            ]
        }

        raise amqp.FailureData(msg)

    def action_read_error_with_custom_data(self, status, answer, accounts):
        print('status: ', repr(status))
        print('answer: ', repr(answer))
        print('accounts: ', repr(accounts))
