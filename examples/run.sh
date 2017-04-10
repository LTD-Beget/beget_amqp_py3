#!/bin/bash
set -e

export PYTHONUNBUFFERED=1

LOGFILE=run.log

>$LOGFILE

./run_with_debugging.py --workers=5 amqp-1 1>>$LOGFILE 2>&1 &

sleep 5

./send/message_for_test_dependence.py &

tail -f $LOGFILE
