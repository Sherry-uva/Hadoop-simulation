'''
columns: jobIndex, arrivalTime, interArrivalTime, inputSize, shuffleSize, outputSize, inputPath, SHFlag, setOfConflictInputPaths

inputSize: the maximum input dataset size of all the jobs using the same input path
SHFlag: 0 - regular job, 1 - shuffle-heavy job
setOfConflictInputPaths: for a shuffle-heavy job, setOfConflictInputPaths means the input paths of all previous shuffle-heavy jobs whose arrival times are within a predifiend time window of the current shuffle-heavy job

'''

import csv, shelve
from math import ceil

def findInputPathInfo(workload):
	inputPathInfo = {}
	countTotal = 0
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
			countTotal += 1
			if job[6] in inputPathInfo:
				if int(job[3]) > inputPathInfo[job[6]]:
					inputPathInfo[job[6]] = int(job[3])
			else:
				inputPathInfo[job[6]] = int(job[3])
	print 'Total number of jobs is ' + str(countTotal)
	return inputPathInfo

	
def processPath(workload, trace):
	inputPathInfo = findInputPathInfo(workload)
	s = shelve.open('input/inputPathInfo_'+trace)
	s.update(inputPathInfo)	
	s.close()
	print 'Total number of regular input paths is ' +  str(len(inputPathInfo))
	numBlocks = 0
	inputSize = 0
	for key, value in inputPathInfo.iteritems():
		numBlocks += int(ceil(value/(1024*1024*128.0)))
		inputSize += value
	print 'Total number of blocks is ' +  str(numBlocks)
	print 'Total size of input datasets is ' +  str(ceil(inputSize/(1024**3))) + ' GB'
















