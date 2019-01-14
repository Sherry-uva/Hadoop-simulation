#!/bin/bash

for count in `seq 1 3`
do
  sbatch simulation.slurm 'duration500sec_SHJRatio0%_arrivalRate0.2' $count
done








