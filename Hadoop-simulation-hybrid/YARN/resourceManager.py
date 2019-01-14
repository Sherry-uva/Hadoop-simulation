# Upon receiving resource requests from AMs, enqueue the requests to corresponding queues
# Responsible for maintaining queues, when a container is allocated by cntrAllo,
# cntrAllo will inform RM about the allocation and RM will update the queues.

import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
import simpy, csv
import numpy as np
from math import ceil, floor
import operator, shelve, random
from itertools import cycle
from collections import deque

import globals as G
import log as L
from AM import applicationMaster as AM
import containerAllocator as CA

# all the active AMs, key: jobId, value: AM.AppMaster()
AMs = dict() 
# rack queues (FIFO), each element in a rack queue is a (jobId, #containers) pair
rackQueues = []
for r in G.SET_RACK_N0:
	rackQueues.append([])
# rack-queue lengths
rackQueueLen = [[r,0] for r in range(G.NUM_N0)]
# cluster queue is a dictionary. key: jobId, value: unsatisfied requests of the job
clusterQueue = {}
# a FIFO queue for allocating AM containers, each element is the jobId
AMQueue = deque()

def ResourceManager(env, pipeAMtoRM, pipe2SDN, pipe2Epath, trace):
	global AMs, rackQueues, rackQueueLen, clusterQueue, AMQueue
	print str(env.now) + ': Starting ResourceManager...'
	while True:
		msg = yield pipeAMtoRM.get()
		message = msg[1]
		if message[0] == 'idleCntr':
			CA.tryAlloCntrClusterQ(message[1], env.now)
		if message[0] == 'jobArrival':
			s = shelve.open('input/resourceRequestsAll_'+trace, flag='r')
			RR = s[message[1]][0]
			blockLocations = s[message[1]][1]
			s.close()
# 			print blockLocations
			if 'mR' in RR:
				if 'rR' in RR:						
					AMs[message[1]] = AM.AppMaster(env, pipeAMtoRM, [], pipe2Epath, message[1], RR, blockLocations, message[2], 0)
				else:
					AMs[message[1]] = AM.AppMaster(env, pipeAMtoRM, [], pipe2Epath, message[1], RR, blockLocations, message[2], 2)
				CA.timeLastAllo[message[1]] = env.now
			else:
				AMs[message[1]] = AM.AppMaster(env, pipeAMtoRM, pipe2SDN, pipe2Epath, message[1], RR, blockLocations, message[2], 1)
			continue		
		# A new job arrives, request a container for its AM
		if message[0] == 'reqAMCntr':
			print str(env.now) + ': ' + message[1] + ' requests an AM container'
			# request a container for the AM
			AMQueue.append(message[1])
			CA.tryAlloAMCntr(env.now)
			continue
		# updated container requests from a RJ AM		
		if message[0] == 'updatedResourceRequestRJ':
			clusterQueue[message[1]] = message[2]
			continue
		if message[0] == 'cntrFinish':
# 			if 'job34' in AMs and AMs['job34'].reduceRacks != []:
# 				pdb.set_trace()
			CA.allocateOneFreeCntr(message[1], message[2], env.now)
			continue
		# map-container requests from a SHJ AM
		if message[0] == 'mapRequestSHJ':
			rackQueueLen = updateRackQueueLen(rackQueues)
			numRack = int(floor(message[4]/G.PER_RACK_SHUFFLE_THRESHOLD))
			mapRackQLen = [rackQueueLen[k] for k in message[3]]
			# ensure the shuffle data generated per map rack is large enough to use a circuit
			if numRack < len(mapRackQLen):
				sortedQLen = sorted(mapRackQLen, key=operator.itemgetter(1))
				mapRackQLen = [sortedQLen[k] for k in range(numRack)]
			mapRackQLen.sort(key=lambda tup: tup[1])
			numReqMapCntr = enqueueSHJ(message[2], mapRackQLen)
			AMs[message[1]].appMaster_proc.interrupt(['mapReqPerRack', numReqMapCntr])
			# put the map-container requests to the corresponding rack queue
			for k in range(len(numReqMapCntr)):
				rackQueues[numReqMapCntr[k][0]].append([message[1], numReqMapCntr[k][1]])
				CA.tryAlloCntrRackQ(numReqMapCntr[k][0], message[1])
			continue		
		# reduce-container requests from a SHJ AM	
		if message[0] == 'reduceRequestSHJ':
			rackQueueLen = updateRackQueueLen(rackQueues)
			numReqReduceCntr = enqueueSHJReduce(message[2], rackQueueLen)
			reduceRacks = [numReqReduceCntr[k][0] for k in range(len(numReqReduceCntr))]
			AMs[message[1]].appMaster_proc.interrupt(['reduceRacks', numReqReduceCntr])
			# put reduce-container requests to the corresponding rack queues, marking these requests as 'waiting'
			for k in range(len(numReqReduceCntr)):
				rackQueues[numReqReduceCntr[k][0]].append([message[1], numReqReduceCntr[k][1], 'waiting'])
			continue
		# after all the map output is shuffled to one reduce rack, the reduce-contianer requests 
		# on this reduce rack will be marked as 'ready'
		if message[0] == 'updateReduceRequestSHJ':
# 			if message[1] == 'job34':
# 				pdb.set_trace()
			tmp = 0
			while message[1] not in rackQueues[message[2]][tmp]:
				tmp += 1
			rackQueues[message[2]][tmp][2] = 'ready'
			CA.tryAlloCntrRackQ(message[2], message[1])
			continue
		# a job is completed, remove its AM and try to reallocate the AM container
		if message[0] == 'jobCompletion':
			jobId = message[1]
			with open(L.logJobCompletionTime, 'a') as f:
				writer = csv.writer(f)
				writer.writerow( (jobId, str(AMs[jobId].numMaps), str(AMs[jobId].numReduces), str(AMs[jobId].startTime), str(env.now), str(env.now-AMs[jobId].startTime)) )
# 				writer.writerow(AMs[jobId].mapCntr)
# 				writer.writerow(AMs[jobId].reduceCntr)
			with open(L.logTasks, 'a') as f:
				writer = csv.writer(f)
				writer.writerow( (env.now, jobId, 'am', 'finish', CA.AMCntr[jobId]) )
			print str(env.now) + ': ' + jobId + ' finishes and frees AM container cntr' + str(CA.AMCntr[jobId])
			del AMs[jobId]
			if jobId in clusterQueue:
				del clusterQueue[jobId]
			if jobId in CA.timeLastAllo:
				del CA.timeLastAllo[jobId]
			cntrId = CA.AMCntr[jobId]
			del CA.AMCntr[jobId]
			CA.allocateOneFreeCntr(jobId, cntrId, env.now)


# Calculate how many containers requested in each rack
def enqueueSHJ(numTasks, rackQLen):
	numReqCntr = [0 for k in range(len(rackQLen))]
	qLen = [rackQLen[k][1] for k in range(len(rackQLen))]
	numTaskLeft = numTasks
	j = 0
	while qLen[j] < max(qLen) and numTaskLeft > 0:
		j = next(x[0] for x in enumerate(qLen) if x[1] > max(numReqCntr[0], qLen[0]))
		i = 0
		for k in cycle(range(j)):
			numReqCntr[i%j] += 1
			numTaskLeft -= 1
			i += 1
			if (i%j == 0 and numReqCntr[0]+qLen[j-1] == qLen[j]) or numTaskLeft == 0:
				break
	while numTaskLeft > 0: 
		for i in range(len(qLen)):
			numReqCntr[i] += 1
			numTaskLeft -= 1
			if numTaskLeft == 0:
				break
	numRequstedCntr = [[rackQLen[k][0],numReqCntr[k]] for k in range(len(qLen)) if numReqCntr[k] > 0]
	return numRequstedCntr
		
# Select reduce racks and calculate how many containers requested in each reduce rack
def enqueueSHJReduce(numReduces, rackQueueLen):
	if numReduces <= 400:
		numRedRack = 1
	elif numReduces <= 800:
		numRedRack = 2
	else:
		numRedRack = 3
	sortedQLen = sorted(rackQueueLen, key=operator.itemgetter(1))
	qZeroLen = [rackQueueLen[k] for k in range(len(rackQueueLen)) if rackQueueLen[k][1] == 0]
	if len(qZeroLen) >= numRedRack:
		tmp = random.sample(range(len(qZeroLen)), numRedRack)
		reduceRackQLen = [qZeroLen[k] for k in tmp]
	else:
		reduceRackQLen = qZeroLen
		for i in range(numRedRack-len(qZeroLen)):
			reduceRackQLen.append(sortedQLen[len(qZeroLen)+i])
	reduceRackQLen.sort(key=lambda tup: tup[1])
	return enqueueSHJ(numReduces, reduceRackQLen)
	
# Update the lengths of rack queues after new requests are added or containers are allocated
def updateRackQueueLen(rackQueues):
	updatedRackQueueLen = [[r,0] for r in range(G.NUM_N0)]
	for r in G.SET_RACK_N0:
		for rackQ in rackQueues[r]:
			updatedRackQueueLen[r][1] += rackQ[1]
	return updatedRackQueueLen
	



















