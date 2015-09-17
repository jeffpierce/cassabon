#!/bin/sh

if [ -z $1 ]; then
    echo "Usage: $0 [HUP|TERM|INT]"
    exit 1
fi

PID=`ps aux | grep [e]xe/cassabon | awk '{print $2}'`
echo "sending signal $1 to $PID"
kill -s $1 $PID
