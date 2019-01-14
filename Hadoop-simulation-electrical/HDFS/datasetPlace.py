import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
import operator
from math import ceil,floor
import numpy as np
import random 
from itertools import cycle

import globals as G
from input import inputSWIM
 
# constants
COUNT_MAX = 10 # limit the size of a shuffle-heavy dataset (SHD) stored on a single host/rack
NUM_BLOCK_PER_ROUND = 8 # number of blocks of a SHD will be stored on a host per time
NUM_BLOCK_RESERVED_PER_HOST = int(ceil(G.DISK_SIZE_PER_HOST*G.GAMMA))




def shuffleHeavyDataset(numBlocks):
	b = 0 # number of stored blocks of one replica, blocks are stored in order
	numOfStoredReplicas = 0 # track the number of already stored replicas
	test = 0
	blockLocations = np.array([[-1 for i in range(3)] for j in range(numBlocks)]) # B_d, numBlocksx3 matrix
	rackStoreReplicas = set() # B_d1 \cup B_d2 \cup B_d3 
	rackSorted = sorted(G.rackN0DiskAvail.items(), key=operator.itemgetter(1), reverse=True)
# 	rackSorted.extend(sorted(G.rackRegularDiskAvail.items(), key=operator.itemgetter(1), reverse=True))
	#### TODO -- check if there is enough disk space on rack set N0 to store the SHD. 
	#### For now, assume that the disk space is always enough
	i = 0 # index of sorted racks
	limit = numBlocks # used when storing different blocks of two replicas on the same rack
	while i < len(rackSorted) and numOfStoredReplicas < 3:
		limit = numBlocks # used when storing different blocks of two replicas on the same rack
		firstBlockThisRack = b # index of the first block stored on this current rack
		round = 0 
		rack = rackSorted[i][0]
		# skip those racks that have already stored some SHDs but do not have much space left
		# if rackSorted[i][1] < NUM_BLOCK_RESERVED_PER_HOST*G.NUM_HOST_PER_RACK:
# 			if rackSorted[i][0] in G.rackRegularDiskAvail or not bool(G.rackRegularDiskAvail): # even those racks storing only regular datasets do not have much space left, or G.rackRegularDiskAvail is empty, meaning no more racks have much space left
# 				pdb.set_trace()
# 				undoBlockPlace(blockLocations, 1)
# 				return False
# 			i += 1 
# 			continue
		rackStoreReplicas.add(rack)
		hostStart = random.choice(G.hostPerRack[rack])		
		hosts = cycle(G.hostPerRack[rack])
		for host in hosts:
			if host < hostStart and round == 0:
				continue
			if host == hostStart:
				round += 1
			if round > COUNT_MAX:
				break	
			bStar = min(NUM_BLOCK_PER_ROUND, limit-b, max(G.diskAvail[host]-NUM_BLOCK_RESERVED_PER_HOST,0))
			blockLocations[b:b+bStar,numOfStoredReplicas] = np.array([host]*bStar)
			b += bStar
			G.diskAvail[host] -= bStar
			G.numSHDBlocks[host] += bStar
			if b == limit and limit < numBlocks:
				break;
			if b == numBlocks: # finish storing all blocks of one replica
				numOfStoredReplicas += 1
				limit = firstBlockThisRack
				if firstBlockThisRack == 0 or numOfStoredReplicas == 3:
					b = 0
					break	
				else:
					b = 0
		i += 1 
	return (blockLocations, rackStoreReplicas)
	
						
# flag = 1 : SHD, flag = 0 : regular dataset
def updateDiskAvailPerRack(rackStoreReplicas, flag):
	if flag == 1:
		# update the number of SHD blocks stored per rack
		for rack in G.numSHDBlocksPerRack:
			G.numSHDBlocksPerRack[rack] = sum([G.numSHDBlocks[i] for i in G.hostPerRack[rack]])
	for rack in G.rackRegularDiskAvail:
		G.rackRegularDiskAvail[rack] = sum([G.diskAvail[i] for i in G.hostPerRack[rack]])
	for rack in G.rackN0DiskAvail:
		G.rackN0DiskAvail[rack] = sum([G.diskAvail[i] for i in G.hostPerRack[rack]])
	average = (sum(G.rackRegularDiskAvail.values()) + sum(G.rackN0DiskAvail.values()))/G.NUM_RACK
	setRack = [rack for rack in range(G.NUM_N0,G.NUM_RACK)]
	for rack in G.rackN0DiskAvail:
		if G.rackN0DiskAvail[rack] > average*G.BALANCE_FACTOR:
			setRack.append(rack)
	setRack.sort()
	return setRack

# if return False, meaning there is not enough disk space to store 3 replicas of all the blocks
def regularDataset(numBlocks, setRack):
	blockLocations = np.array([[-1 for i in range(3)] for j in range(numBlocks)]) # B_d, numBlocksx3 matrix
	setHost = []
	for rack in G.SET_RACK:
		if rack in setRack:
			setHost.extend(G.hostPerRack[rack])
	for i in range(numBlocks):
		hostExcluded = []
		while True:
			host = random.choice([x for x in setHost if x not in hostExcluded])
			if G.diskAvail[host] > 0:
				blockLocations[i,0] = host
				G.diskAvail[host] -= 1
				break
			hostExcluded.append(host)
			if len(hostExcluded) == len(setHost): # all the hosts in setHost are full
				pdb.set_trace()
				undoBlockPlace(blockLocations, 0)
				return False
		rackExcluded = [G.MAPPING_HOST_TO_RACK[host]]
		# whether the rack storing the 1st replica is in setRack
# 		if G.MAPPING_HOST_TO_RACK[host] in setRack:
# 			flag = 0
# 		else:
# 			flag = 1
		while True:
			# find a different rack from the one storing the first replica
			rack = random.choice([x for x in setRack if x not in rackExcluded])
			rackExcluded.append(rack)
			# find a host to store the 2nd replica
			hostExcluded = []
			while blockLocations[i,1] == -1: # when the second rack can only store the 2nd replica, this while loop will be skipped
				host = random.choice([x for x in G.hostPerRack[rack] if x not in hostExcluded])
				hostExcluded.append(host)
				if G.diskAvail[host] > 0:
					blockLocations[i,1] = host
					G.diskAvail[host] -= 1
					break
				if len(hostExcluded) == G.NUM_HOST_PER_RACK:
					break
			if len(hostExcluded) == G.NUM_HOST_PER_RACK: # there is no free space on the current randomly chosen rack 
				continue
			while True:
				host = random.choice([x for x in G.hostPerRack[rack] if x not in hostExcluded])
				if G.diskAvail[host] > 0:
					blockLocations[i,2] = host
					G.diskAvail[host] -= 1
					break
				hostExcluded.append(host)
				if len(hostExcluded) == G.NUM_HOST_PER_RACK:
					break
			if len(rackExcluded) == len(setRack):
				pdb.set_trace()
				undoBlockPlace(blockLocations, 0)
				return False
			if blockLocations[i,2] != -1:
				break
	return blockLocations

def undoBlockPlace(blockLocations,flag):
	for i in range(blockLocations.shape[0]):
		for j in range(blockLocations.shape[1]):
			host = blockLocations[i, j]
			if host != -1:
				G.diskAvail[host] += 1
				if flag == 1:
					G.numSHDBlocks[host] -= 1
					
	
workload = "/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/SWIM/FB-2010_samples_24_times_1hr_withInputPaths_0.tsv"


def populateHDFS(workload):
	SHDResult = []
	setRack = G.SET_RACK
	oldSetRack = G.SET_RACK
	datasets = inputSWIM.inputShuffleSizes(workload) # input and shuffle sizes of all the datasets, in bytes
	jobIndex = 0
	blockLocationAllDataset = dict()
	totalSize = 0
	SHDSize = 0
	numTotalBlocks = 0
	numShuffleHeavy = 0
	first = 1
	# tmp = 0 
	for dataset in datasets:
		print dataset[0] + ': ' + str(dataset[3])
	# 	tmp += 1
	# 	if tmp < 4564:
	# 		jobIndex += 1
	# 		continue
		totalSize += dataset[3]
		numBlocks = int(ceil(dataset[3]/(1024.0*1024*128)))
		numTotalBlocks += numBlocks
		if jobIndex == 6000:
			pdb.set_trace()
		if dataset[3] != 0:
			if dataset[4] > dataset[3] and dataset[4] > 1024*1024*1024:
# 			if dataset[4] > 1024*1024*1024:
				print 'shuffle-heavy job' + str(jobIndex)+': ' + str(numBlocks)
				SHDResult.append(dataset)
				if len(SHDResult) == 1:
					SHDResult[0][2] = SHDResult[0][1]
				else:
					SHDResult[numShuffleHeavy][2] = int(SHDResult[numShuffleHeavy][1]) - int(SHDResult[numShuffleHeavy-1][1])
				SHDSize += dataset[3]
# 				pdb.set_trace()
				B_d, Z_d = shuffleHeavyDataset(numBlocks)
				SHDResult[numShuffleHeavy].append(Z_d)
				flag = 1
				numShuffleHeavy += 1
			else:
				B_d = regularDataset(numBlocks, setRack)
				Z_d = []
				flag = 0
			setRack = updateDiskAvailPerRack(Z_d, flag)
			if first == 1 and flag == 1:
				print G.rackN0DiskAvail
				first = 0
		else:
			B_d = []
# 		blockLocationAllDataset[jobIndex] = B_d
		jobIndex += 1
		if jobIndex%1000 == 0:
			print totalSize/(1024.0*1024*1024)
			print G.rackN0DiskAvail
			print G.rackRegularDiskAvail
		if jobIndex == 5000:
			pdb.set_trace()
			
	# 	if flag == 1:
	# 		print G.rackN0DiskAvail
	# 		print G.rackRegularDiskAvail
	print totalSize/(1024.0*1024*1024)
	print numTotalBlocks	
	print 'max: ' + str(max(G.diskAvail))
	print 'min: ' + str(min(G.diskAvail))
	print G.rackN0DiskAvail
	print G.rackRegularDiskAvail
	f = open("SHDResult.txt", "w")
	f.write("\n".join([",".join([str(n) for n in item]) for item in SHDResult]))
	f.close()
	
# 	
# print len(blockLocationAllDataset)

# B_d, Z_d = shuffleHeavyDataset(700)
# print Z_d
# updateDiskAvailPerRack(Z_d, 1)
# print regularDataset(100)
# updateDiskAvailPerRack([], 0)
# print G.rackN0DiskAvail
# print G.rackRegularDiskAvail
# 
# f = open("output.txt", "w")
# f.write("\n".join([",".join([str(n) for n in item]) for item in B_d.tolist()]))
# f.close()



def populateHDFSRandom(workload):
	setRack = G.SET_RACK
	oldSetRack = G.SET_RACK
	datasets = inputSWIM.inputShuffleSizes(workload)[0:6000] # input and shuffle sizes of all the datasets, in bytes
	jobIndex = 0
	blockLocationAllDataset = dict()
	totalSize = 0
	SHDSize = 0
	numTotalBlocks = 0
	numShuffleHeavy = 0
	first = 1
	# tmp = 0 
	count = 0
	unchosenJobs = range(6000)
	while len(unchosenJobs) > 0:
		index = random.choice(unchosenJobs)
		unchosenJobs.remove(index)
		dataset = datasets[index]
		print str(count) + ' - ' + dataset[0] + ': ' + str(dataset[3])
	# 	tmp += 1
	# 	if tmp < 4564:
	# 		jobIndex += 1
	# 		continue
		totalSize += dataset[3]
		numBlocks = int(ceil(dataset[3]/(1024.0*1024*128)))
		numTotalBlocks += numBlocks
		if jobIndex == 6000:
			pdb.set_trace()
		if dataset[3] != 0:
			if dataset[4] > dataset[3] and dataset[4] > 1024*1024*1024:
# 			if dataset[4] > 1024*1024*1024:
				print 'shuffle-heavy job' + str(jobIndex)+': ' + str(numBlocks)
				SHDSize += dataset[3]
# 				pdb.set_trace()
				numShuffleHeavy += 1
				B_d, Z_d = shuffleHeavyDataset(numBlocks)
				flag = 1
			else:
				B_d = regularDataset(numBlocks, setRack)
				Z_d = []
				flag = 0
			setRack = updateDiskAvailPerRack(Z_d, flag)
# 			if len(setRack) != len(oldSetRack):
# 				pdb.set_trace()
# 				print setRack
# 				print oldSetRack
# 			oldSetRack = setRack[:]
			if first == 1 and flag == 1:
				print G.rackN0DiskAvail
				first = 0
		else:
			B_d = []
		count += 1
		
			
populateHDFS(workload)
			
# try:
# 	populateHDFS(workload)
# except:
# 	type, value, tb = sys.exc_info()
# 	traceback.print_exc()
# 	pdb.post_mortem(tb)
# 	
# workloads = ["/Users/sherry/Box Sync/Hadoop-simulation/input/SWIM/FB-2009_samples_24_times_1hr_0.tsv", "/Users/sherry/Box Sync/Hadoop-simulation/input/SWIM/FB-2009_samples_24_times_1hr_1.tsv", "/Users/sherry/Box Sync/Hadoop-simulation/input/SWIM/FB-2010_samples_24_times_1hr_0.tsv", "/Users/sherry/Box Sync/Hadoop-simulation/input/SWIM/FB-2010_samples_24_times_1hr_withInputPaths_0.tsv"]
# 
# workloadName = ['2009_0', '2009_1', '2010_0', '2010_1'] 
# i = 0
# for workload in workloads:
# 	datasets = inputSWIM.inputShuffleSizes(workload)
# 	totalSize = 0
# 	numBlocks = 0
# 	for dataset in datasets:
# 		totalSize += dataset[0]
# 		numBlocks += int(ceil(dataset[0]/(1024.0*1024*128)))
# 	print workloadName[i] + ': ' + str(totalSize/(1024.0*1024*1024))
# 	print 'number of blocks: ' + str(numBlocks)
# 	i += 1
# 	
# datasets = inputSWIM.inputShuffleSizes(workloads[2])
# totalSize = 0
# numBlocks = 0
# count = 0
# for dataset in datasets:
# 	numBlocks = int(ceil(dataset[0]/(1024.0*1024*128)))
# 	if numBlocks > 32000:
# 		print 'job' + str(count) + ': ' + str(numBlocks)
# 	count += 1
# 	if count == 6000:
# 		break
		
		










