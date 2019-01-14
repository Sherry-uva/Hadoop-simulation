#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -t 1:00:00
#SBATCH -p largemem
#SBATCH -A hntes_group

module load python/2.7.6
python preProcessPath.py
