'''
columns: jobIndex, arrivalTime, interArrivalTime, inputSize, shuffleSize, outputSize, inputPath, SHFlag, setOfConflictInputPaths

inputSize: the maximum input dataset size of all the jobs using the same input path
SHFlag: 0 - regular job, 1 - shuffle-heavy job
setOfConflictInputPaths: for a shuffle-heavy job, setOfConflictInputPaths means the input paths of all previous shuffle-heavy jobs whose arrival times are within a predifiend time window of the current shuffle-heavy job

'''
import sys, csv, pdb
from math import ceil
import shelve

# workload = "/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/100jobs_SHJRatio5%_arrivalRate0.2.csv"

'''
find the input paths of all the SHDs in the workload
return dictionary SHInputPathInfo, key: inputPath, value: [inputSize, [jobIndices]]
inputSize: the maximum input dataset size of all the jobs using the same input path
jobIndices: indices of jobs using the same input path
''' 
def findInputPathInfo(workload):
	SHInputPathInfo = {}
	regularInputPathInfo = {}
	countSH = 0
	countTotal = 0
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
			print job[0]
			countTotal += 1
			# SHJ condition: shuffle size larger than 2 GB; ratio of input size to shuffle size is less than 1000
			if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000: 
				countSH += 1
				if job[6] not in SHInputPathInfo:
					SHInputPathInfo[job[6]] = [int(job[3]), [job[0]]]	
				else:
					SHInputPathInfo[job[6]][1].append(job[0])	
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
			# some regular jobs might have the same input path as SHJs
			# and these regular jobs might have larger input data sizes
			if job[6] in SHInputPathInfo: 
				if int(job[3]) > SHInputPathInfo[job[6]][0]:
					SHInputPathInfo[job[6]][0] = int(job[3])
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
			if job[6] not in SHInputPathInfo:
				if job[6] in regularInputPathInfo:
					if int(job[3]) > regularInputPathInfo[job[6]]:
						regularInputPathInfo[job[6]] = int(job[3])
				else:
					regularInputPathInfo[job[6]] = int(job[3])
	print 'Total number of shuffle-heavy paths is ' + str(len(SHInputPathInfo))
	print 'Total number of shuffle-heavy jobs is ' + str(countSH)
	print 'Total number of jobs is ' + str(countTotal)
	return (SHInputPathInfo, regularInputPathInfo)

# return a list of shuffle heavy jobs: [jobIndex, arrivalTime, inputPath]	
def SHJobArrivalTime(workload):
	SHJobArrivalTimes = []
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
			if int(job[4]) > 2*1024**3:
				tmp = [job[0], int(job[1]), job[6]]
				SHJobArrivalTimes.append(tmp)
	print 'Total number of shuffle-heavy jobs is ' + str(len(SHJobArrivalTimes))
	return SHJobArrivalTimes
	

def processPath(workload, trace):	
	SHInputPathInfo, regularInputPathInfo = findInputPathInfo(workload)
	s = shelve.open('input/SHInputPathInfo_'+trace)
	s.update(SHInputPathInfo)	
	s.close()
	print 'Total number of shuffle-heavy input paths is ' +  str(len(SHInputPathInfo))
	s = shelve.open('input/regularInputPathInfo_'+trace)
	s.update(regularInputPathInfo)	
	s.close()
	print 'Total number of regular input paths is ' +  str(len(regularInputPathInfo))
	numBlocks = 0
	inputSize = 0
	for key, value in SHInputPathInfo.iteritems():
		inputSize += value[0]
		numBlocks += int(ceil(value[0]/(1024*1024*128.0)))
	print 'Total number of shuffle-heavy dataset blocks is ' +  str(numBlocks)
	print 'Total size of shuffle-heavy input datasets is ' +  str(ceil(inputSize/(1024**3))) + ' GB'
	for key, value in regularInputPathInfo.iteritems():
		numBlocks += int(ceil(value/(1024*1024*128.0)))
		inputSize += value
	print 'Total number of blocks is ' +  str(numBlocks)
	print 'Total size of input datasets is ' +  str(ceil(inputSize/(1024**3))) + ' GB'
















