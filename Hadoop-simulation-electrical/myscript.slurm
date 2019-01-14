#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -t 00:20:00
#SBATCH -p largemem
#SBATCH -A hntes_group

module load python/2.7.6
stdbuf -o0 -e0 time python main.py
#stdbuf -o0 -e0 time python -m cProfile -s cumulative main.py
