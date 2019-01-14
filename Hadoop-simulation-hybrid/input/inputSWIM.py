from math import ceil,floor
import csv, pdb

# workload = "/Users/sherry/Box Sync/Hadoop-simulation/input/SWIM/FB-2009_samples_24_times_1hr_0.tsv"

def inputShuffleSizes(workload):
	inputShuffleSize = []
	with open(workload,'r') as csvfile :
		reader = csv.reader(csvfile)
		for line in reader:
			inputShuffleSize.append([line[0], line[1]] + [int(line[i]) for i in range(2,5)] + [line[5], line[6]])
	return inputShuffleSize

# inputShuffleSize = inputShuffleSizes(workload)
# print sum([inputShuffleSize[i][0] for i in range(len(inputShuffleSize))])
	

