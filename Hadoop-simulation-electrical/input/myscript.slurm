#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -t 00:10:00
#SBATCH -p serial
#SBATCH -A hntes_group

module load python/2.7.6
python preProcessPath.py
