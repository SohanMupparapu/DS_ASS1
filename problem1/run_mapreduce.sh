#!/bin/bash

############################################
# Arguments
############################################
INPUT_FILE=$1
OUTPUT_DIR=$2

############################################
# Configuration
############################################
NUM_MAPPERS=4
NUM_REDUCERS=4

MAP_DIR=map_parts
MAP_OUT_DIR=map_out
SHUFFLE_DIR=shuffle
GROUP_DIR=grouped

############################################
# Setup
############################################
mkdir -p "$MAP_DIR" "$MAP_OUT_DIR" "$SHUFFLE_DIR" "$GROUP_DIR" "$OUTPUT_DIR"

############################################
# NODE 0 : MAP + COMBINE + SHUFFLE
############################################
if [ "$SLURM_NODEID" -eq 0 ]; then
    echo "[Node 0] MAP phase starting"

    rm -f "$MAP_DIR"/* "$MAP_OUT_DIR"/* "$SHUFFLE_DIR"/* "$GROUP_DIR"/* map_done.flag

    ########################################
    # Split input
    ########################################
    TOTAL_LINES=$(wc -l < "$INPUT_FILE")
    LINES_PER_MAPPER=$(( (TOTAL_LINES + NUM_MAPPERS - 1) / NUM_MAPPERS ))

    split -l "$LINES_PER_MAPPER" \
          --numeric-suffixes=0 \
          --suffix-length=1 \
          "$INPUT_FILE" \
          "$MAP_DIR/map_part_"

    ########################################
    # Run mappers + combiners
    ########################################
    for part in "$MAP_DIR"/map_part_*; do
        i=$(basename "$part" | sed "s/map_part_//")

        srun --exclusive -n 1 bash -c "
            cat '$part' \
            | python3 mapper.py \
            | python3 combiner.py \
            > '$MAP_OUT_DIR/map_out_$i.txt'
        " &
    done
    wait

    echo "[Node 0] MAP phase complete"

    ########################################
    # SHUFFLE = MERGE + SORT + GROUP
    ########################################
    echo "[Node 0] SHUFFLE (sort + group) starting"

    # Merge all mapper outputs
    cat "$MAP_OUT_DIR"/map_out_*.txt > "$SHUFFLE_DIR/merged.txt"

    # Sort by key
    sort "$SHUFFLE_DIR/merged.txt" > "$SHUFFLE_DIR/sorted.txt"

    # Group values per key
    awk '
    {
        key = $1
        val = $2

        if (NR == 1) {
            curr = key
            values = val
        }
        else if (key == curr) {
            values = values "," val
        }
        else {
            print curr "\t" values ""
            curr = key
            values = val
        }
    }
    END {
        if (NR > 0)
            print curr "\t" values ""
    }
    ' "$SHUFFLE_DIR/sorted.txt" > "$GROUP_DIR/grouped.txt"

    ########################################
    # Split grouped keys for reducers
    ########################################
    TOTAL_KEYS=$(wc -l < "$GROUP_DIR/grouped.txt")
    KEYS_PER_REDUCER=$(( (TOTAL_KEYS + NUM_REDUCERS - 1) / NUM_REDUCERS ))

    split -l "$KEYS_PER_REDUCER" \
          --numeric-suffixes=0 \
          --suffix-length=1 \
          "$GROUP_DIR/grouped.txt" \
          "$GROUP_DIR/reduce_part_"

    echo "[Node 0] SHUFFLE complete"

    touch map_done.flag
fi

############################################
# BARRIER
############################################
while [ ! -f map_done.flag ]; do
    sleep 1
done

############################################
# NODE 1 : REDUCE
############################################
if [ "$SLURM_NODEID" -eq 1 ]; then
    echo "[Node 1] REDUCE phase starting"

    for f in "$GROUP_DIR"/reduce_part_*; do
        i=$(basename "$f" | sed "s/reduce_part_//")

        srun --exclusive -n 1 bash -c "
            cat '$f' | python3 reducer.py > '$OUTPUT_DIR/part-0000$i'
        " &
    done
    wait

    echo "[Node 1] REDUCE phase complete"
fi
