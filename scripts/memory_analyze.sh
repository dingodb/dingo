#!/bin/bash

PID=${1}

convert_to_kb() {
    SIZE=$1
    # Check if SIZE is a number with optional g/m/k at the end
    if [[ ! $SIZE =~ ^[0-9]+[gmk]?$ ]]; then
        echo 0
        return
    fi

    if [[ $SIZE == *g ]]; then
        # Convert from GB to KB
        NUMBER=$(echo $SIZE | sed 's/g//')
        echo $(echo "$NUMBER * 1024 * 1024" | bc)
    elif [[ $SIZE == *m ]]; then
        # Convert from MB to KB
        NUMBER=$(echo $SIZE | sed 's/m//')
        echo $(echo "$NUMBER * 1024" | bc)
    elif [[ $SIZE == *k ]]; then
        # Already in KB, just remove the 'k'
        echo $SIZE | sed 's/k//'
    else
        # No unit, assume KB
        echo $SIZE
    fi
}

while true; do
    # 使用 top 命令获取特定进程的信息
    # -b: 批处理模式，适用于脚本
    # -n 1: 只获取一次更新
    # -p $PID: 只监控特定的进程
    PROCESS_INFO=$(top -b -n 1 -p $PID | grep $PID)

    if [ -z "$PROCESS_INFO" ]; then
        echo "Process $PID not found."
        exit 1
    else
        RES_RAW=$(echo $PROCESS_INFO | awk '{print $6}')
        SHR_RAW=$(echo $PROCESS_INFO | awk '{print $7}')

        # Convert to KB
        RES=$(convert_to_kb $RES_RAW)
        SHR=$(convert_to_kb $SHR_RAW)

        TIME=$(date +"%Y-%m-%d %H:%M:%S")$(awk -v var=$(date +"%N") 'BEGIN{printf(",%03d", var / 1000000)}')

        echo "${TIME} RES: ${RES} KB, SHR: ${SHR} KB"
        echo "${TIME} RES: ${RES} KB, SHR: ${SHR} KB" >> ${PID}_memory.log
    fi

    sleep 1
done