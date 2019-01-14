#!/bin/bash

for ratio in 5 10 20
do
	for lambda in 0.05 0.1 0.2
	do
		python input/jobMixGenerator.py 40 $ratio $lambda
	done
done







