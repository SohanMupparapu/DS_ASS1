#!/bin/bash

LOG_FILE=$1
INPUT_FILE=$2

if [ $# -ne 2 ]; then
    echo "Usage: $0 <log_file.csv> <input_file>"
    exit 1
fi

############################
# FILE SIZE
############################
INPUT_SIZE_BYTES=$(stat -c %s "$INPUT_FILE")
INPUT_SIZE_MB=$(awk "BEGIN {printf \"%.2f\", $INPUT_SIZE_BYTES/1024/1024}")

############################
# EXTRACT TIMES (ms)
############################
get_time () {
    grep "^$1," "$LOG_FILE" | awk -F',' '{print $4}'
}

INIT_TIME_MS=$(get_time init)
MAP_TIME_MS=$(get_time map)
SHUFFLE_TIME_MS=$(get_time shuffle)
REDUCE_TIME_MS=$(get_time reduce)
TOTAL_TIME_MS=$(get_time total)

############################
# THROUGHPUT CALCULATIONS
############################
map_throughput () {
    awk "BEGIN {
        if ($MAP_TIME_MS > 0)
            printf \"%.2f\", $INPUT_SIZE_MB / ($MAP_TIME_MS / 1000)
        else
            print 0
    }"
}

reduce_throughput () {
    awk "BEGIN {
        if ($REDUCE_TIME_MS > 0)
            printf \"%.2f\", $INPUT_SIZE_MB / ($REDUCE_TIME_MS / 1000)
        else
            print 0
    }"
}

MAP_TP=$(map_throughput)
REDUCE_TP=$(reduce_throughput)

############################
# OUTPUT METRICS
############################
echo "=============================="
echo " MapReduce Performance Metrics"
echo "=============================="
echo "Input file           : $INPUT_FILE"
echo "Input size           : ${INPUT_SIZE_MB} MB"
echo
echo "Task initialization  : ${INIT_TIME_MS} ms"
echo "Map throughput       : ${MAP_TP} MB/s"
echo "Reduce throughput    : ${REDUCE_TP} MB/s"
echo
echo "Disk I/O time        : ${SHUFFLE_TIME_MS} ms"
echo "Network overhead     : N/A (local filesystem)"
echo
echo "Total execution time : ${TOTAL_TIME_MS} ms"
echo "=============================="
