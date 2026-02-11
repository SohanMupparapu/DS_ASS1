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

############################################
# ITERATIVE MAPREDUCE
############################################
for ((iter=1; iter<=ITERATIONS; iter++)); do
    echo "========== ITERATION $iter =========="

    ########################################
    # MAP + COMBINE + SHUFFLE (NODE 0 ONLY)
    ########################################
        echo "Iteration $iter: MAP phase"

        # Clean previous iteration data
        rm -f "$MAP_DIR"/* "$MAP_OUT_DIR"/* "$SHUFFLE_DIR"/* "$GROUP_DIR"/*

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

            # Mapper -> Combiner
            python3 mapper.py < "$IN" | python3 combiner.py > "$OUT"
        '

        ####################################
        # SHUFFLE = MERGE + SORT
        ####################################
        cat "$MAP_OUT_DIR"/map_out_*.txt > "$SHUFFLE_DIR/merged.txt"
        sort "$SHUFFLE_DIR/merged.txt" > "$SHUFFLE_DIR/sorted.txt"

        ####################################
        # GROUP: Combine PR and ADJ into single line per node
        ####################################
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

        srun --ntasks=$NUM_REDUCERS --exclusive bash -c '
            RID=$SLURM_PROCID
            IN="'"$GROUP_DIR"'/reduce_part_${RID}.txt"
            OUT="'"$WORK_DIR"'/iter_'$iter'_out_${RID}.txt"

            python3 reducer.py < "$IN" > "$OUT"
        '

        # Merge reducer outputs
        cat "$WORK_DIR"/iter_${iter}_out_*.txt \
            > "$WORK_DIR/iter_${iter}_output.txt"

        echo "Iteration $iter: REDUCE complete"

    ########################################
    # Barrier - wait before next iteration
    ########################################
    srun --ntasks=$SLURM_NTASKS --wait=0 true

    ########################################
    # Prepare input for next iteration
    ########################################
    CURRENT_INPUT="$WORK_DIR/iter_${iter}_output.txt"
done

############################################
# Final output
############################################
    cp "$CURRENT_INPUT" "$OUTPUT_DIR/result.txt"
    echo "Final output written"

echo "[Node $SLURM_NODEID] Job finished"