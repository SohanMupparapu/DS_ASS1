#!/bin/bash
set -e

############################################
# Ensure correct working directory
############################################
cd "$SLURM_SUBMIT_DIR" || exit 1

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
LOG_DIR=logs
TOTAL_DOCS=$(wc -l < "$INPUT_FILE")
echo "$TOTAL_DOCS"
export TOTAL_DOCS

############################################
# Setup
############################################
mkdir -p "$MAP_DIR" "$MAP_OUT_DIR" "$SHUFFLE_DIR" "$GROUP_DIR" "$OUTPUT_DIR" "$LOG_DIR"

# Per-node logging
exec > "$LOG_DIR/node_${SLURM_NODEID}.log" 2>&1

echo "[Node $SLURM_NODEID] Started on $(hostname)"

############################################
# NODE 0 : MAP + COMBINE + SHUFFLE
############################################


    rm -f "$MAP_DIR"/* "$MAP_OUT_DIR"/* "$SHUFFLE_DIR"/* "$GROUP_DIR"/*

    ########################################
    # Split input
    ########################################

    split -n l/$NUM_MAPPERS \
        --numeric-suffixes=0 \
        --suffix-length=1 \
        --additional-suffix=.txt \
        "$INPUT_FILE" \
        "$MAP_DIR/map_part_"

    ########################################
    # Run mappers + combiners (SLURM-correct)
    ########################################
    echo "[Node $SLURM_NODEID] Running mappers"

    srun --ntasks=$NUM_MAPPERS --exclusive bash -c '
        i=$SLURM_PROCID
        part="'"$MAP_DIR"'/map_part_${i}.txt"
        out="'"$MAP_OUT_DIR"'/map_out_${i}.txt"

        echo "[Mapper $i] Processing $part on $(hostname)"

        cat "$part" \
        | python3 mapper.py \
        | python3 combiner.py \
        > "$out"
    '

    echo "[Node $SLURM_NODEID] MAP phase complete"

    ########################################
    # SHUFFLE = MERGE + SORT + GROUP
    ########################################
    echo "[Node $SLURM_NODEID] SHUFFLE phase starting"

    cat "$MAP_OUT_DIR"/map_out_*.txt > "$SHUFFLE_DIR/merged.txt"
    sort "$SHUFFLE_DIR/merged.txt" > "$SHUFFLE_DIR/sorted.txt"

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
            print curr "\t" values
            curr = key
            values = val
        }
    }
    END {
        if (NR > 0)
            print curr "\t" values
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
        --additional-suffix=.txt \
        "$GROUP_DIR/grouped.txt" \
        "$GROUP_DIR/reduce_part_"

    echo "[Node $SLURM_NODEID] SHUFFLE complete"

############################################
# BARRIER (REAL SLURM BARRIER)
############################################
echo "[Node $SLURM_NODEID] Waiting at barrier"
srun --ntasks=2 --wait=0 true
echo "[Node $SLURM_NODEID] Passed barrier"

############################################
# NODE 1 : REDUCE
############################################
    echo "[Node $SLURM_NODEID] REDUCE phase starting"

    srun --ntasks=$NUM_REDUCERS --exclusive bash -c '
        i=$SLURM_PROCID
        part="'"$GROUP_DIR"'/reduce_part_${i}.txt"
        out="'"$OUTPUT_DIR"'/part-0000${i}.txt"

        echo "[Reducer $i] Processing $part on $(hostname)"

        python3 reducer.py < "$part" > "$out"
    '
    cat "$OUTPUT_DIR"/part-*.txt > "$OUTPUT_DIR"/result.txt

    echo "[Node ] REDUCE phase complete"

echo "[Node $SLURM_NODEID] Job finished"