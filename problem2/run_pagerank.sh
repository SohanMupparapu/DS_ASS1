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
echo "Total nodes: $TOTAL_NODES"
export TOTAL_NODES
export DAMPING

PROFILE_LOG="profile_metrics.csv"
echo "iteration,total_time,map_time,shuffle_time,reduce_time,io_time" > $PROFILE_LOG

############################################
# ITERATIVE MAPREDUCE
############################################
for ((iter=1; iter<=ITERATIONS; iter++)); do
    ITER_START=$(date +%s.%N)

    echo "========== ITERATION $iter =========="

    ########################################
    # MAP + COMBINE + SHUFFLE (NODE 0 ONLY)
    ########################################
        echo "Iteration $iter: MAP phase"

        # Clean previous iteration data
        rm -f "$MAP_DIR"/* "$MAP_OUT_DIR"/* "$SHUFFLE_DIR"/* "$GROUP_DIR"/*

        MAP_START=$(date +%s.%N)
        ####################################
        # Split input among mappers
        ####################################
        SPLIT_START=$(date +%s.%N)
        split -n l/$NUM_MAPPERS \
            --numeric-suffixes=0 \
            --suffix-length=1 \
            --additional-suffix=.txt \
            "$CURRENT_INPUT" \
            "$MAP_DIR/map_part_"
        SPLIT_END=$(date +%s.%N)
        SPLIT_TIME=$(echo "$SPLIT_END - $SPLIT_START" | bc)


        ####################################
        # Run mappers + combiners
        ####################################
        srun --ntasks=$NUM_MAPPERS --exclusive bash -c '
            MID=$SLURM_PROCID
            IN="'"$MAP_DIR"'/map_part_${MID}.txt"
            OUT="'"$MAP_OUT_DIR"'/map_out_${MID}.txt"

            # Mapper -> Combiner
            python3 mapper.py < "$IN" | python3 combiner.py > "$OUT"
        '
        echo "Iteration $iter: MAP+SHUFFLE complete"
        MAP_END=$(date +%s.%N)
        MAP_TIME=$(echo "$MAP_END - $MAP_START" | bc)

        ####################################
        # SHUFFLE = MERGE + SORT
        ####################################
        SHUFFLE_START=$(date +%s.%N)
        cat "$MAP_OUT_DIR"/map_out_*.txt > "$SHUFFLE_DIR/merged.txt"
        sort "$SHUFFLE_DIR/merged.txt" > "$SHUFFLE_DIR/sorted.txt"
        SHUFFLE_END=$(date +%s.%N)
        SHUFFLE_TIME=$(echo "$SHUFFLE_END - $SHUFFLE_START" | bc)
        ####################################
        # GROUP: Combine PR and ADJ into single line per node
        ####################################
        GROUP_START=$(date +%s.%N)
        awk '
        BEGIN { curr = ""; pr_list = ""; adj = "" }
        {
            key = $1
            tag = $2
            val = $3
            
            # New key encountered
            if (key != curr && curr != "") {
                # Emit previous key with all info in ONE line
                print curr "\t" adj "\t" pr_list
                pr_list = ""
                adj = ""
            }
            
            curr = key
            
            if (tag == "PR") {
                # Accumulate PR values as comma-separated list
                if (pr_list == "") {
                    pr_list = val
                } else {
                    pr_list = pr_list "," val
                }
            } else if (tag == "ADJ") {
                adj = val
            }
        }
        END {
            # Emit last key
            if (curr != "") {
                print curr "\t" adj "\t" pr_list
            }
        }' "$SHUFFLE_DIR/sorted.txt" > "$GROUP_DIR/grouped.txt"
        GROUP_END=$(date +%s.%N)
        GROUP_TIME=$(echo "$GROUP_END - $GROUP_START" | bc)
        ####################################
        # Split grouped data for reducers
        ####################################
        split -n l/$NUM_REDUCERS \
            --numeric-suffixes=0 \
            --suffix-length=1 \
            --additional-suffix=.txt \
            "$GROUP_DIR/grouped.txt" \
            "$GROUP_DIR/reduce_part_"

        echo "Iteration $iter: MAP+SHUFFLE complete"

        SHUFFLE_TIME=$(echo "$SHUFFLE_END - $SHUFFLE_START" | bc)
    ########################################
    # Barrier - wait for all nodes
    ########################################
    echo "[Node $SLURM_NODEID] Waiting at barrier (iteration $iter)"
    srun --ntasks=$SLURM_NTASKS --wait=0 true
    echo "[Node $SLURM_NODEID] Passed barrier (iteration $iter)"

    ########################################
    # REDUCE 
    ########################################
        echo "Iteration $iter: REDUCE phase"
        REDUCE_START=$(date +%s.%N)

        srun --ntasks=$NUM_REDUCERS --exclusive bash -c '
            RID=$SLURM_PROCID
            IN="'"$GROUP_DIR"'/reduce_part_${RID}.txt"
            OUT="'"$WORK_DIR"'/iter_'$iter'_out_${RID}.txt"

            python3 reducer.py < "$IN" > "$OUT"
        '

        # Merge reducer outputs
        MERGE_START=$(date +%s.%N)
        cat "$WORK_DIR"/iter_${iter}_out_*.txt \
            > "$WORK_DIR/iter_${iter}_output.txt"
        MERGE_END=$(date +%s.%N)
        MERGE_TIME=$(echo "$MERGE_END - $MERGE_START" | bc)

        REDUCE_END=$(date +%s.%N)
        REDUCE_TIME=$(echo "$REDUCE_END - $REDUCE_START" | bc)

        echo "Iteration $iter: REDUCE complete"

    ########################################
    # Barrier - wait before next iteration
    ########################################
    srun --ntasks=$SLURM_NTASKS --wait=0 true

    ########################################
    # Prepare input for next iteration
    ########################################
    CURRENT_INPUT="$WORK_DIR/iter_${iter}_output.txt"
    ITER_END=$(date +%s.%N)
    TOTAL_TIME=$(echo "$ITER_END - $ITER_START" | bc)
    IO_TIME=$(echo "$SPLIT_TIME + $MERGE_TIME + $GROUP_TIME + $SHUFFLE_TIME" | bc)
    echo "$iter,$TOTAL_TIME,$MAP_TIME,$SHUFFLE_TIME,$REDUCE_TIME,$IO_TIME" >> $PROFILE_LOG
done

############################################
# Final output
############################################
    cp "$CURRENT_INPUT" "$OUTPUT_DIR/result.txt"
    echo "Final output written"

echo "[Node $SLURM_NODEID] Job finished"