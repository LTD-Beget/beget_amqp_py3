# -*- coding: utf-8 -*-

from ..helpers.argument import Argument


class ExceptionHandler(Exception):
    """
    Исключение произошедшее в Handler
    """

    def __init__(self, message, code=1, trace=None):
        """
        :type message: basestring
        :type code: int
        :type trace: basestring
        """
        self.message = Argument.check_type(message, str, 'No message', strict_type=str)
        self.code = Argument.check_type(code, int, 1, strict_type=int)
        self.trace = Argument.check_type(trace, str, '', strict_type=(type(None), str))

        Exception.__init__(self, self.message, self.code, self.trace)
