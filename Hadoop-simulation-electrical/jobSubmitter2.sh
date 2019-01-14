#!/bin/bash

simTime=(18000 11000 5500 9000 6000 4000 5500 4000 2500)
index=0
for ratio in 5 10 20
do
	for lambda in 0.05 0.1 0.2
	do
		for count in `seq 1 1`
		do
			output="numSHJ40_SHJRatio$ratio%_arrivalRate$lambda.out$count"
  			stdbuf -o0 -e0 time python main.py 40 $ratio $lambda $count ${simTime[index]} >> $output &
		done
		index=$[index+1]
	done
done










