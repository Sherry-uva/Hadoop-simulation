'''
Store datasets in FB-2009_samples_24_times_1hr_0_first50jobs.tsv onto HDFS. 
Assume datasets with the same input path are the same, and will be stored onto HDFS only once.
Input datasets used by shuffle-heavy jobs whose submission times are close to each other 
will be stored on different sets of racks to avoid conflicts.  
'''



import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
import operator
from math import ceil,floor
import numpy as np
import random, pickle, shelve
from itertools import cycle

import globals as G
from input import inputSWIM



# constants
COUNT_MAX = 10 # limit the size of a shuffle-heavy dataset (SHD) stored on a single host/rack
NUM_BLOCK_PER_ROUND = 8 # number of blocks of a SHD will be stored on a host per time
NUM_BLOCK_RESERVED_PER_HOST = int(ceil(G.DISK_SIZE_PER_HOST*G.GAMMA))


# workload = "/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/100jobs_SHJRatio5%_arrivalRate0.2.csv"



def shuffleHeavyDataset(numBlocks, blockSize, numReplicas):
	blockLocations = np.array([[-1 for i in range(numReplicas)] for j in range(numBlocks)]) # B_d, numBlocks x numReplicas matrix
	rackSorted = sorted(G.rackN0DiskAvail.items(), key=operator.itemgetter(1), reverse=True)
	rackStoreReplicas = [rackSorted[i][0] for i in range(numReplicas)] # use the rack with larger free space to store the dataset
	for index in range(numReplicas):
		numBlockStored = 0 # number of stored blocks of one replica, blocks are stored in order
		rack = rackSorted[index][0]
		hosts = cycle(G.hostPerRack[rack])
		for host in hosts:
			blockLocations[numBlockStored, index] = host
			G.diskAvail[host] -= blockSize
			G.numSHDBlocks[host] += blockSize
			numBlockStored += 1
			if numBlockStored == numBlocks:
				break
	return (blockLocations, rackStoreReplicas)	
						
# flag = 1 : SHD, flag = 0 : regular dataset
def updateDiskAvailPerRack(flag):
	if flag == 1:
		# update the number of SHD blocks stored per rack
		for rack in G.numSHDBlocksPerRack:
			G.numSHDBlocksPerRack[rack] = sum([G.numSHDBlocks[i] for i in G.hostPerRack[rack]])
	for rack in G.rackRegularDiskAvail:
		G.rackRegularDiskAvail[rack] = sum([G.diskAvail[i] for i in G.hostPerRack[rack]])
	for rack in G.rackN0DiskAvail:
		G.rackN0DiskAvail[rack] = sum([G.diskAvail[i] for i in G.hostPerRack[rack]])
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


def undoBlockPlace(blockLocations,flag, blockSize):
	for i in range(blockLocations.shape[0]):
		for j in range(blockLocations.shape[1]):
			host = blockLocations[i, j]
			if host != -1:
				G.diskAvail[host] += blockSize
				if flag == 1:
					G.numSHDBlocks[host] -= 1
					

def populateHDFS(workload, trace):
	largeRJs = [line.strip() for line in open('input/largeRJ_'+trace+'.txt', 'r')]
	largeSHJs = [line.strip() for line in open('input/largeSHJ_'+trace+'.txt', 'r')]
	setRack = G.SET_RACK
	oldSetRack = G.SET_RACK
	datasets = inputSWIM.inputShuffleSizes(workload) # input and shuffle sizes of all the datasets, in bytes
	jobIndex = 0
	blockLocationAll = dict() # block location matrix B_d of all the input paths, key: inputPath, value: B_d
	mappingInputPathToRack = dict() # key: input path, value: for regular datasets, []; for SHDs, set of racks storing the dataset
	SHInputPathInfo = shelve.open('input/SHInputPathInfo_'+trace, flag='r')
	regularInputPathInfo = shelve.open('input/regularInputPathInfo_'+trace, flag='r')
	totalSize = 0
	SHDSize = 0
	numTotalBlocks = 0
	numShuffleHeavy = 0
	first = 1
	blockLocationAll = dict()
	for job in datasets:
		print job[0] + ' input size: ' + str(job[3])
		if job[6] in mappingInputPathToRack: # dataset has already been stored
			jobIndex += 1
			continue
		if job[6] in SHInputPathInfo: # SHD
# 			print 'shuffle-heavy job' + str(jobIndex)+' has ' + str(numBlocks) + ' blocks with input path ' + job[6]
			SHDSize += SHInputPathInfo[job[6]][0]
			totalSize += SHInputPathInfo[job[6]][0]
			if job[0] not in largeSHJs:
				numBlocks = int(ceil(SHInputPathInfo[job[6]][0]/(1024.0*1024*128)))
				B_d, Z_d = shuffleHeavyDataset(numBlocks, 1, G.NUM_REPLICAS)
			else:
				numBlocks = int(ceil(SHInputPathInfo[job[6]][0]/(1024.0**3)))
				B_d, Z_d = shuffleHeavyDataset(numBlocks, 8, G.NUM_REPLICAS)
			print 'SHJ ' + job[0] + ' has input stored in rack ' + str(Z_d[0]) 
			mappingInputPathToRack[job[6]] = Z_d
			flag = 1
			numShuffleHeavy += 1
		else: # input paths in regularInputPathInfo for regular datasets
			mappingInputPathToRack[job[6]] = []
			inputSize = regularInputPathInfo[job[6]]
			if inputSize == 0:
				numBlocks = 0
				B_d = []
			else:
				totalSize += inputSize
				if job[0] not in largeRJs:
					numBlocks = int(ceil(inputSize/(1024.0*1024*128)))
					B_d = regularDataset(numBlocks, setRack, 1, G.NUM_REPLICAS)
				else:
					numBlocks = int(ceil(inputSize/(1024.0**3)))
					B_d = regularDataset(numBlocks, setRack, 8, G.NUM_REPLICAS)
			flag = 0
		setRack = updateDiskAvailPerRack(flag)
		blockLocationAll[job[6]] = B_d
		print B_d
		numTotalBlocks += numBlocks
		jobIndex += 1
	s = shelve.open('input/rackSetOfInputPath_'+trace)
	s.update(mappingInputPathToRack)	
	s.close()
	s = shelve.open('input/blockLocationAll_'+trace)
	s.update(blockLocationAll)	
	s.close()
	# 	if flag == 1:
	# 		print G.rackShuffleDiskAvail
	# 		print G.rackRegularDiskAvail
	print 'total size: ' + str(int(totalSize/(1024.0*1024*1024))) + ' GB'
	print 'SHD size: ' + str(int(SHDSize/(1024.0*1024*1024))) + ' GB'
	print 'total number of blocks: ' + str(numTotalBlocks)	
	print 'max: ' + str(max(G.diskAvail))
	print 'min: ' + str(min(G.diskAvail))
	print G.rackRegularDiskAvail
	print G.rackN0DiskAvail
	print G.numSHDBlocksPerRack
# 	print blockLocationAll['inputPath1']
#  	print blockLocationAll['inputPath2']

			
			
# try:
# 	populateHDFS(workload)
# except:
# 	type, value, tb = sys.exc_info()
# 	traceback.print_exc()
# 	pdb.post_mortem(tb)











