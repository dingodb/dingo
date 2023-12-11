#!/bin/bash

PROCESS_PID=$1

if [ -z "$PROCESS_PID" ]; then
    echo "PID NOT FOUND"
    exit 1
fi

if [ ! -d "/proc/$PROCESS_PID" ]; then
    echo "PID $PROCESS_PID DOESN'T EXIST"
    exit 1
fi

echo "PID $PROCESS_PID STATUS"

for THREAD_DIR in /proc/$PROCESS_PID/task/*; do
    THREAD_ID=$(basename $THREAD_DIR)
    MEMORY_INFO=$(grep VmRSS $THREAD_DIR/status)
    MEMORY_KB=$(grep VmRSS $THREAD_DIR/status | awk '{print $2}')
    MEMORY_MB=$(echo "scale=2; $MEMORY_KB/1024" | bc)
    THREAD_NAME=$(grep -P '^Name:' $THREAD_DIR/status | awk '{print $2}')  
    echo "thread_id: $THREAD_ID, thread_name: $THREAD_NAME, memory: $MEMORY_MB MB"
done
