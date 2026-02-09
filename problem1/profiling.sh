run_mapreduce() {
    INPUT_FILE=$1
    OUTPUT_DIR=$2
    LOG_FILE=$3

    NUM_MAPPERS=4
    NUM_REDUCERS=4

    MAP_DIR=map_parts
    MAP_OUT_DIR=map_out
    SHUFFLE_DIR=shuffle
    GROUP_DIR=grouped

    mkdir -p "$MAP_DIR" "$MAP_OUT_DIR" "$SHUFFLE_DIR" "$GROUP_DIR" "$OUTPUT_DIR"

    echo "phase,start_ms,end_ms,duration_ms" >> "$LOG_FILE"

    ############################
    # TASK INITIALIZATION TIME
    ############################
    t0=$(now_ms)
    sleep 0.2
    t1=$(now_ms)
    echo "init,$t0,$t1,$((t1-t0))" >> "$LOG_FILE"

    ############################
    # MAP PHASE
    ############################
    t_map_start=$(now_ms)

    split -n l/$NUM_MAPPERS "$INPUT_FILE" "$MAP_DIR/map_part_"

    for part in "$MAP_DIR"/map_part_*; do
        srun --exclusive -n 1 bash -c "
            cat '$part' \
            | python3 mapper.py \
            | python3 combiner.py \
            > '$MAP_OUT_DIR/$(basename $part).out'
        " &
    done
    wait

    t_map_end=$(now_ms)
    echo "map,$t_map_start,$t_map_end,$((t_map_end-t_map_start))" >> "$LOG_FILE"

    ############################
    # SHUFFLE + SORT + GROUP
    ############################
    t_shuffle_start=$(now_ms)

    cat "$MAP_OUT_DIR"/*.out > "$SHUFFLE_DIR/merged.txt"
    sort "$SHUFFLE_DIR/merged.txt" > "$SHUFFLE_DIR/sorted.txt"

    awk '
    {
        if (NR==1) { k=$1; v=$2 }
        else if ($1==k) { v=v","$2 }
        else { print k"\t["v"]"; k=$1; v=$2 }
    }
    END { print k"\t["v"]" }
    ' "$SHUFFLE_DIR/sorted.txt" > "$GROUP_DIR/grouped.txt"

    t_shuffle_end=$(now_ms)
    echo "shuffle,$t_shuffle_start,$t_shuffle_end,$((t_shuffle_end-t_shuffle_start))" >> "$LOG_FILE"

    ############################
    # REDUCE PHASE
    ############################
    t_reduce_start=$(now_ms)

    split -n l/$NUM_REDUCERS "$GROUP_DIR/grouped.txt" "$GROUP_DIR/reduce_part_"

    for part in "$GROUP_DIR"/reduce_part_*; do
        srun --exclusive -n 1 bash -c "
            python3 reducer.py < '$part' > '$OUTPUT_DIR/$(basename $part).out'
        " &
    done
    wait

    t_reduce_end=$(now_ms)
    echo "reduce,$t_reduce_start,$t_reduce_end,$((t_reduce_end-t_reduce_start))" >> "$LOG_FILE"

    ############################
    # TOTAL TIME
    ############################
    echo "total,$t0,$t_reduce_end,$((t_reduce_end-t0))" >> "$LOG_FILE"
}
generate_random_file() {
    SIZE_MB=$1
    OUT_FILE=$2

    # ~6 bytes per word including space → ~170k words per MB
    WORDS_PER_MB=170000
    TOTAL_WORDS=$((SIZE_MB * WORDS_PER_MB))

    shuf -n "$TOTAL_WORDS" /usr/dict/words 2>/dev/null \
        || shuf -n "$TOTAL_WORDS" /usr/share/dict/words \
        || yes "word" | head -n "$TOTAL_WORDS" \
        > "$OUT_FILE"
}

mkdir -p inputs outputs logs

for SIZE in 1 10 100; do
    INPUT_FILE="inputs/input_${SIZE}MB.txt"
    OUTPUT_DIR="outputs/out_${SIZE}MB"
    LOG_FILE="logs/log_${SIZE}MB.csv"

    echo "Generating ${SIZE}MB input..."
    generate_random_file "$SIZE" "$INPUT_FILE"

    echo "Running MapReduce for ${SIZE}MB..."
    run_mapreduce "$INPUT_FILE" "$OUTPUT_DIR" "$LOG_FILE"
done
