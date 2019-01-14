'''
Store datasets in FB-2009_samples_24_times_1hr_0_first50jobs.tsv onto HDFS. 
Assume datasets with the same input path are the same, and will be stored onto HDFS only once.
Input datasets used by shuffle-heavy jobs whose submission times are close to each other 
will be stored on different sets of racks to avoid conflicts.  
'''



import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
import operator
from math import ceil,floor
import numpy as np
import random, pickle, shelve
from itertools import cycle

import globals as G
import log as L
from input import inputSWIM


# workload = '/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/' + sys.argv[1]
						
def updateDiskAvailPerRack():
	for rack in G.rackRegularDiskAvail:
		G.rackRegularDiskAvail[rack] = sum([G.diskAvail[i] for i in G.hostPerRack[rack]])
	return G.SET_RACK[:]

# if return False, meaning there is not enough disk space to store 3 replicas of all the blocks
# blockSize: 1 or 8 (for large regular jobs)
def regularDataset(numBlocks, setRack, blockSize, numReplicas):
	blockLocations = np.array([[-1 for i in range(numReplicas)] for j in range(numBlocks)]) # B_d, numBlocksx3 matrix
	setHost = []
	for rack in G.SET_RACK:
		if rack in setRack:
			setHost.extend(G.hostPerRack[rack])
	for i in range(numBlocks):
		hostExcluded = []
		while True:
			host = random.choice([x for x in setHost if x not in hostExcluded])
			if G.diskAvail[host] >= blockSize:
				blockLocations[i,0] = host
				G.diskAvail[host] -= blockSize
				break
			hostExcluded.append(host)
			if len(hostExcluded) == len(setHost) and blockLocations[i, numReplicas-1] == -1: # all the hosts in setHost are full
				pdb.set_trace()
				undoBlockPlace(blockLocations, 0, blockSize)
				return False
		if numReplicas > 1:
			rackExcluded = [G.MAPPING_HOST_TO_RACK[host]]
			while True:
				# find a different rack from the one storing the first replica
				rack = random.choice([x for x in setRack if x not in rackExcluded])
				rackExcluded.append(rack)
				# find a host to store the 2nd replica
				hostExcluded = []
				while blockLocations[i,1] == -1: # when the second rack can only store the 2nd replica, this while loop will be skipped
					host = random.choice([x for x in G.hostPerRack[rack] if x not in hostExcluded])
					if G.diskAvail[host] >= blockSize:
						blockLocations[i,1] = host
						G.diskAvail[host] -= blockSize
						break
					hostExcluded.append(host)
					if len(hostExcluded) == G.NUM_HOST_PER_RACK:
						break
				if blockLocations[i,1] == -1: # there is no free space on the current randomly chosen rack 
					continue
				if numReplicas == 3:
					while True:
						host = random.choice([x for x in G.hostPerRack[rack] if x not in hostExcluded])
						if G.diskAvail[host] >= blockSize:
							blockLocations[i,2] = host
							G.diskAvail[host] -= blockSize
							break
						hostExcluded.append(host)
						if len(hostExcluded) == G.NUM_HOST_PER_RACK:
							break
				if len(rackExcluded) == len(setRack) and blockLocations[i, numReplicas-1] == -1:
					pdb.set_trace()
					undoBlockPlace(blockLocations, 0, blockSize)
					return False
				if blockLocations[i, numReplicas-1] != -1:
					break
	return blockLocations


def undoBlockPlace(blockLocations, blockSize):
	for i in range(blockLocations.shape[0]):
		for j in range(blockLocations.shape[1]):
			host = blockLocations[i, j]
			if host != -1:
				G.diskAvail[host] += blockSize
			


def populateHDFS(workload, trace):
	setRack = G.SET_RACK
	oldSetRack = G.SET_RACK
	datasets = inputSWIM.inputShuffleSizes(workload) # input and shuffle sizes of all the datasets, in bytes
	jobIndex = 0
	blockLocationAll = dict() # block location matrix B_d of all the input paths, key: inputPath, value: B_d
	inputPathStored = [] 
	inputPathInfo = shelve.open('input/inputPathInfo_'+trace, flag='r')
	totalSize = 0
	SHDSize = 0
	numTotalBlocks = 0
	numShuffleHeavy = 0
	first = 1
	blockLocationAll = dict()
	for job in datasets:
		print job[0] + ' input size: ' + str(job[3])
		if job[6] in inputPathStored: # dataset has already been stored
			jobIndex += 1
			continue
		inputPathStored.append(job[6])
		inputSize = inputPathInfo[job[6]]
		if inputSize == 0:
			numBlocks = 0
			B_d = []
		else:
			totalSize += inputSize
			if job[0] not in L.largeJobs:
				numBlocks = int(ceil(inputSize/(1024.0*1024*128)))
				B_d = regularDataset(numBlocks, setRack, 1, G.NUM_REPLICAS)
			else:
				numBlocks = int(ceil(inputSize/(1024.0**3)))
				B_d = regularDataset(numBlocks, setRack, 8, G.NUM_REPLICAS)
		setRack = updateDiskAvailPerRack()
		blockLocationAll[job[6]] = B_d
		print B_d
		numTotalBlocks += numBlocks
		jobIndex += 1	
	s = shelve.open('input/blockLocationAll_'+trace)
	s.update(blockLocationAll)	
	s.close()

	print 'total size: ' + str(int(totalSize/(1024.0*1024*1024))) + ' GB'
	print 'SHD size: ' + str(int(SHDSize/(1024.0*1024*1024))) + ' GB'
	print 'total number of blocks: ' + str(numTotalBlocks)	
	print 'max: ' + str(max(G.diskAvail))
	print 'min: ' + str(min(G.diskAvail))
	print G.rackRegularDiskAvail


			
# try:
# 	populateHDFS(workload)
# except:
# 	type, value, tb = sys.exc_info()
# 	traceback.print_exc()
# 	pdb.post_mortem(tb)











