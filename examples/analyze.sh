#!/bin/bash

for log in run.log run2.log run_single5.log; do
    echo
    echo
    echo "RUN: $log"

    for cond in before after; do
        echo
        echo "$cond:" | awk '{print toupper($0)}'
        cat $cond-$log | grep RUN | grep -E 'get-message' | perl -ne 'm/(AmqpWorker-\S+).*(\(\d+\) RUN(?: \{\d+\})? \[.*\] ).*("sleep_time": \d+)?/ && printf "%s %-32s %-24s\n", $1, $2, $3'
    done
done

echo

