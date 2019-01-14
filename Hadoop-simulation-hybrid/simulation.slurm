#!/bin/bash

#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -t 01:00:00
#SBATCH -p serial
#SBATCH -A hntes_group

module load python/2.7.6
stdbuf -o0 -e0 time python main.py $1 $2

