#!/bin/bash

INPUT_FILE=$1
OUTPUT_DIR=$2

# Parallelism parameters
NUM_MAPPERS=4
NUM_REDUCERS=4

# Directories
MAP_DIR=map_parts
MAP_OUT_DIR=map_out
REDUCE_DIR=reduce_parts

mkdir -p "$MAP_DIR" "$MAP_OUT_DIR" "$REDUCE_DIR" "$OUTPUT_DIR"

############################################
# NODE 0 : MAP PHASE
############################################
if [ "$SLURM_NODEID" -eq 0 ]; then
    echo "[Node 0] Starting map phase"

    rm -f "$MAP_DIR"/* "$MAP_OUT_DIR"/*

    # 1. Shuffle documents to mappers (hash on document id)
    awk -v M=$NUM_MAPPERS '
    {
        h = 0
        for (i = 1; i <= length($1); i++)
            h = (h * 31 + substr($1, i, 1)) % M
        print $0 >> "'"$MAP_DIR"'/map_part_" h ".txt"
    }' "$INPUT_FILE"

    # 2. Run mappers in parallel (4 CPUs)
    ls "$MAP_DIR"/map_part_*.txt | \
    xargs -n 1 -P $NUM_MAPPERS -I {} bash -c '
        i=$(basename {} | sed "s/map_part_//")
        cat {} | python3 mapper.py | sort > "'"$MAP_OUT_DIR"'/map_out_$i.txt"
    '

    echo "[Node 0] Map phase complete"
fi

############################################
# Barrier (filesystem-based)
############################################
if [ "$SLURM_NODEID" -eq 0 ]; then
    touch map_done.flag
fi

while [ ! -f map_done.flag ]; do
    sleep 1
done

############################################
# NODE 1 : SHUFFLE + REDUCE
############################################
if [ "$SLURM_NODEID" -eq 1 ]; then
    echo "[Node 1] Starting reduce phase"

    rm -f "$REDUCE_DIR"/*

    # 3. Shuffle mapper outputs to reducers (hash on key)
    for f in "$MAP_OUT_DIR"/map_out_*.txt; do
        awk -v R=$NUM_REDUCERS '
        {
            split($0, a, "\t")
            h = 0
            for (i = 1; i <= length(a[1]); i++)
                h = (h * 31 + substr(a[1], i, 1)) % R
            print $0 >> "'"$REDUCE_DIR"'/reduce_part_" h ".txt"
        }' "$f"
    done

    # 4. Run reducers in parallel (4 CPUs)
    ls "$REDUCE_DIR"/reduce_part_*.txt | \
    xargs -n 1 -P $NUM_REDUCERS -I {} bash -c '
        i=$(basename {} | sed "s/reduce_part_//")
        sort {} | python3 reducer.py > "'"$OUTPUT_DIR"'/part-0000$i"
    '

    echo "[Node 1] Reduce phase complete"
fi
