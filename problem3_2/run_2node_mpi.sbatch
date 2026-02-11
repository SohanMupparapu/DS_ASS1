#!/bin/bash
#SBATCH --job-name=mpi_job
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00
#SBATCH --output=mpi_%j.out
#SBATCH --error=mpi_%j.err

INPUT_FILE=$1
NUMBER_PROCS=$2

bash run_mpi.sh "$INPUT_FILE" "$NUMBER_PROCS"
