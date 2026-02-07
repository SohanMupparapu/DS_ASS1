#!/bin/bash
#SBATCH --job-name=mapreduce_2node
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:10:00
#SBATCH --output=mr_%j.out
#SBATCH --error=mr_%j.err

module load python || true

INPUT_FILE=$1
OUTPUT_DIR=$2

srun bash run_mapreduce.sh "$INPUT_FILE" "$OUTPUT_DIR"
