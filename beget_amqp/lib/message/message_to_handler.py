# -*- coding: utf-8 -*-
from ..helpers.argument import Argument


class MessageToHandler(object):
    """
    Сообщение которое передается в Handler

    Задача:
      - Строго опредилить список передаваемых Handler данных.
    """

    def __init__(self, controller, action, params=None):
        assert isinstance(controller, str), 'controller must be a string, but is: %s' % repr(controller)
        self.controller = controller

        assert isinstance(action, str), 'action must be a string, but is: %s' % repr(action)
        self.action = action

        self.params = Argument.check_type(params, dict, {}, strict_type=(type(None), list, dict))
