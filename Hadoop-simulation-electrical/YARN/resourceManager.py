# Upon receiving resource requests from AMs, enqueue the requests to corresponding queues
# Responsible for maintaining queues, when a container is allocated by cntrAllo,
# cntrAllo will inform RM about the allocation and RM will update the queues.

import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
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
# cluster queue is a dictionary. key: jobId, value: unsatisfied requests of the job
clusterQueue = {}
# a FIFO queue for allocating AM containers, each element is the jobId
AMQueue = deque()

def ResourceManager(env, pipeAMtoRM, pipe2Epath, trace):
	global AMs, rackQueues, rackQueueLen, clusterQueue, AMQueue
	print str(env.now) + ': Starting ResourceManager...'
	while True:
		msg = yield pipeAMtoRM.get()
		message = msg[1]
		if message[0] == 'idleCntr':
# 			print str(env.now) + ': RM receives a heartbeat from NM ' + str(message[1])
			CA.tryAlloCntrClusterQ(message[1], env.now)
		if message[0] == 'jobArrival':
			s = shelve.open('input/resourceRequestsAll_'+trace, flag='r')
			RR = s[message[1]][0]
			blockLocations = s[message[1]][1]
			s.close()
# 			print blockLocations
			if 'rR' in RR:						
				AMs[message[1]] = AM.AppMaster(env, pipeAMtoRM, pipe2Epath, message[1], RR, blockLocations, message[2], 0)
			else:
				AMs[message[1]] = AM.AppMaster(env, pipeAMtoRM, pipe2Epath, message[1], RR, blockLocations, message[2], 1)
			CA.timeLastAllo[message[1]] = env.now
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
			CA.allocateOneFreeCntr(message[1], message[2], env.now)
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






















