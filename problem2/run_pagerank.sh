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
ITERATIONS=10
DAMPING=0.85

MAP_DIR=map_parts
MAP_OUT_DIR=map_out
SHUFFLE_DIR=shuffle
GROUP_DIR=grouped
LOG_DIR=logs
WORK_DIR=work

############################################
# Setup
############################################
mkdir -p "$MAP_DIR" "$MAP_OUT_DIR" "$SHUFFLE_DIR" \
         "$GROUP_DIR" "$OUTPUT_DIR" "$LOG_DIR" "$WORK_DIR"

exec > "$LOG_DIR/node_${SLURM_NODEID}.log" 2>&1
echo "[Node $SLURM_NODEID] Started on $(hostname)"

############################################
# Initial input
############################################
CURRENT_INPUT="$INPUT_FILE"
TOTAL_NODES=$(wc -l < "$INPUT_FILE")
export TOTAL_NODES
export DAMPING

############################################
# ITERATIVE MAPREDUCE
############################################
for ((iter=1; iter<=ITERATIONS; iter++)); do
    echo "========== ITERATION $iter =========="

    ########################################
    # MAP + COMBINE + SHUFFLE (NODE 0)
    ########################################
        echo "[Node 0] Iteration $iter: MAP phase"

        rm -f "$MAP_DIR"/* "$MAP_OUT_DIR"/* \
              "$SHUFFLE_DIR"/* "$GROUP_DIR"/*

        ####################################
        # Split input among mappers
        ####################################
        split -n l/$NUM_MAPPERS \
            --numeric-suffixes=0 \
            --suffix-length=1 \
            --additional-suffix=.txt \
            "$CURRENT_INPUT" \
            "$MAP_DIR/map_part_"

        ####################################
        # Run mappers + combiners
        ####################################
        srun --ntasks=$NUM_MAPPERS --exclusive bash -c '
            MID=$SLURM_PROCID
            IN="'"$MAP_DIR"'/map_part_${MID}.txt"
            OUT="'"$MAP_OUT_DIR"'/map_out_${MID}.txt"

            python3 mapper.py < "$IN" \
            | python3 combiner.py \
            > "$OUT"
        '

        ####################################
        # SHUFFLE = MERGE + SORT + GROUP
        ####################################
        cat "$MAP_OUT_DIR"/map_out_*.txt > "$SHUFFLE_DIR/merged.txt"
        sort "$SHUFFLE_DIR/merged.txt" > "$SHUFFLE_DIR/sorted.txt"

        awk '
        {
            key=$1; tag=$2; val=$3
            if (NR==1) {
                curr=key; adj=""; sum=""
            }
            if (key!=curr) {
                print curr "\t" sum "\t" adj
                curr=key; adj=""; sum=""
            }
            if (tag=="PR") {
                sum = (sum=="" ? val : sum "," val)
            }
            else if (tag=="ADJ") {
                adj = val
            }
        }
        END {
            if (NR>0)
                print curr "\t" sum "\t" adj
        }' "$SHUFFLE_DIR/sorted.txt" > "$GROUP_DIR/grouped.txt"

        ####################################
        # Split grouped keys for reducers
        ####################################
        split -n l/$NUM_REDUCERS \
            --numeric-suffixes=0 \
            --suffix-length=1 \
            --additional-suffix=.txt \
            "$GROUP_DIR/grouped.txt" \
            "$GROUP_DIR/reduce_part_"

        touch "$WORK_DIR/iter_${iter}_map_done"
        echo "[Node 0] Iteration $iter: MAP+SHUFFLE complete"

    ########################################
    # Barrier
    ########################################
    echo "[Node $SLURM_NODEID] Waiting at barrier (iteration $iter)"
    srun --ntasks=$SLURM_NTASKS --wait=0 true
    echo "[Node $SLURM_NODEID] Passed barrier (iteration $iter)"


    ########################################
    # REDUCE (NODE 1)
    ########################################
        echo "[Node 1] Iteration $iter: REDUCE phase"

        srun --ntasks=$NUM_REDUCERS --exclusive bash -c '
            RID=$SLURM_PROCID
            IN="'"$GROUP_DIR"'/reduce_part_${RID}.txt"
            OUT="'"$WORK_DIR"'/iter_'$iter'_out_${RID}.txt"

            python3 reducer.py < "$IN" > "$OUT"
        '

        cat "$WORK_DIR"/iter_${iter}_out_*.txt \
            > "$WORK_DIR/iter_${iter}_output.txt"

        echo "[Node 1] Iteration $iter: REDUCE complete"

    ########################################
    # Prepare input for next iteration
    ########################################
    CURRENT_INPUT="$WORK_DIR/iter_${iter}_output.txt"
done

############################################
# Final output
############################################
    cp "$CURRENT_INPUT" "$OUTPUT_DIR/result.txt"
    echo "[Node 1] Final output written"


echo "[Node $SLURM_NODEID] Job finished"
