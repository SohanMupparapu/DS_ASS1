#!/bin/bash

INPUT_FILE=$1
NUM_PROCESSES=$2
module load hpcx-2.7.0/hpcx-ompi

mpirun --bind-to none --mca coll ^hcoll -np $NUM_PROCESSES ./mpi_graph $INPUT_FILE