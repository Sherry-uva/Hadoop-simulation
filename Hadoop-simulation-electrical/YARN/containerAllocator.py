import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
import random, simpy, csv
from operator import add, itemgetter 
import numpy as np

from globals import SET_HOST, hostPerRack, NUM_HOST_PER_RACK, NUM_CNTR_PER_HOST, RACK_LOCALITY_DELAY, \
MAPPING_CNTR_TO_HOST, MAPPING_CNTR_TO_RACK, MAPPING_HOST_TO_RACK, NM_HEARTBEAT_INTERVAL, CPU_UTIL_UPDATE_MAX
import log as L
import resourceManager as RM
# from resourceManager import rackQueues, clusterQueue, AMQueue, AMs

# each element indicate whether a container is in use or idle, 0: idle, 1: in use
cntrStatus = {host:[0 for k in range(NUM_CNTR_PER_HOST)] for host in SET_HOST}
	
# number of allocated containers for each regular job, used for fair scheduling
# key: jobId, value: number of allocated containers
numCntrPerJob = {}

# the time passed since the last allocation of a node/rack-local container of each regular job
timeLastAllo = {}

# containers that are allocated for AMs
AMCntr = dict()


# periodically sending heartbeats to the RM about each node's idle containers
class NodeManager:
	def __init__(self, env, pipe2RM, hostId):
		self.startTime = np.random.uniform(0, NM_HEARTBEAT_INTERVAL)
		self.hostId = hostId
		self.nodeManager_proc = env.process(self.nodeManager(env, pipe2RM))
		self.counter = 1
		self.cpuUtil = [0 for k in range(NUM_CNTR_PER_HOST)]
	
	def nodeManager(self, env, pipe2RM):
		yield env.timeout(self.startTime)
		print str(env.now) + ': start node manager of host ' + str(self.hostId)
		self.cpuUtil = cntrStatus[self.hostId]
		if 0 in cntrStatus[self.hostId]: 
			pipe2RM.put(simpy.PriorityItem(2, ['idleCntr', self.hostId]))
		while True:
			yield env.timeout(NM_HEARTBEAT_INTERVAL)
			if self.counter < CPU_UTIL_UPDATE_MAX:
				self.cpuUtil = map(add, self.cpuUtil, cntrStatus[self.hostId])
				self.counter += 1
			else:
				with open(L.logCPUUtil, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (self.hostId, env.now, self.cpuUtil) )
				self.counter = 1
				self.cpuUtil = cntrStatus[self.hostId]
			# if there is at least one idle container, send a message to the RM; otherwise do nothing
			if 0 in cntrStatus[self.hostId]: 
				pipe2RM.put(simpy.PriorityItem(2, ['idleCntr', self.hostId]))
				

''' allocate a newly freed container 
    priority of queues: RM.AMQueue > RM.rackQueues > RM.clusterQueue
    jobId is the job that frees this container 
'''    
def allocateOneFreeCntr(jobId, cntrId, currTime):
	global RM, cntrStatus, numCntrPerJob, timeLastAllo, AMCntr, NUM_CNTR_PER_HOST, MAPPING_CNTR_TO_RACK, MAPPING_CNTR_TO_HOST, NUM_N0
	# update the number of allocated containers of job jobId
	if jobId in numCntrPerJob:
		if numCntrPerJob[jobId] > 1:
			numCntrPerJob[jobId] = numCntrPerJob[jobId]-1		
		else:
			del numCntrPerJob[jobId]
	if len(RM.AMQueue) > 0:
		jobId = RM.AMQueue.pop()
		AMCntr[jobId] = cntrId
		if jobId in timeLastAllo:
			timeLastAllo[jobId] = currTime
			numCntrPerJob[jobId] = 1 # AM container is the first container allocated to a job
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (currTime, jobId, 'am', 'start', cntrId) )
		# inform the job about its AM-container allocation
		RM.AMs[jobId].startAM.succeed()
		return
	rackId = MAPPING_CNTR_TO_RACK[cntrId]
	# try to allocate the container to a cluster-queue request
	numCntrPerJobSorted = sorted(numCntrPerJob.items(), key=itemgetter(1))
	jobId, taskType = allocateCntrClusterQ(numCntrPerJobSorted, rackId, currTime)
	if jobId != -1:
		if jobId in RM.clusterQueue and RM.clusterQueue[jobId] == {}:
			del RM.clusterQueue[jobId]
		numCntrPerJob[jobId] += 1
		RM.AMs[jobId].appMaster_proc.interrupt(['containerAllocation', [taskType, cntrId]])
	# if the container cannot be allocated to any jobs, then update its status as idle
	else: 
		hostId = MAPPING_CNTR_TO_HOST[cntrId]
		tmp = cntrId - NUM_CNTR_PER_HOST*hostId
		cntrStatus[hostId][tmp] = 0


def allocateCntrClusterQ(numCntrPerJobSorted, rackId, currTime):
	global RM, timeLastAllo
	for job in numCntrPerJobSorted:
		jobId = job[0]
		if jobId not in timeLastAllo: # SHJ
			continue
		if jobId in RM.clusterQueue and RM.clusterQueue[jobId] == {}:
			del RM.clusterQueue[jobId]
			continue
		if jobId not in RM.clusterQueue:
			continue
		requests = RM.clusterQueue[jobId]
		# if a reduce container is requested, it has higher priority than a map-cntr request
		if 'rR' in requests:
			numReqRedCntr = requests['rR']['*']
			if numReqRedCntr > 0:
				if numReqRedCntr > 1:
					RM.clusterQueue[jobId]['rR']['*'] = numReqRedCntr-1
				else:
					del RM.clusterQueue[jobId]['rR']
# 				timeLastAllo[jobId] = currTime
				return (jobId, 'r')
			else:
				del RM.clusterQueue[jobId]['rR']
		if 'mR' in requests:
			if requests['mR']['*'] == 0:
				del RM.clusterQueue[jobId]['mR']
				return (-1, -1)
			updatedRequests, flagLastAlloTime = canAlloCntrToRJ(rackId, requests['mR'].copy(), currTime-timeLastAllo[jobId])
			if updatedRequests['*'] < requests['mR']['*']:
				if flagLastAlloTime == 1:
					timeLastAllo[jobId] = currTime
				if updatedRequests['*'] == 0:
					del RM.clusterQueue[jobId]['mR']
				else:
					RM.clusterQueue[jobId]['mR'] = updatedRequests
# 					print RM.clusterQueue[jobId]['mR'] 
				return (jobId, 'm')
	# no job in the cluster queue can be allocated with this container
	return (-1, -1)

	
# decide whether the container could be allocated to a job as a map container	
def canAlloCntrToRJ(rackId, mapRequests, timePassedSinceLastAllo):
	global RACK_LOCALITY_DELAY
	# node/rack-local request
	if rackId in mapRequests:
		flag = 1
		if mapRequests[rackId] > 1:
			mapRequests[rackId] = mapRequests[rackId]-1
		else:
			del mapRequests[rackId]
		mapRequests['*'] = mapRequests['*']-1
	else:
		flag = 0
		if len(mapRequests) == 1:
			# zero-input job, mapRequests only has a '*' entry
			# no need to delay the container allocation to wait for a local container
			mapRequests['*'] = mapRequests['*']-1
		else:
			if timePassedSinceLastAllo > RACK_LOCALITY_DELAY:
	# 			print 'Allocating a no-local container...'
	# 			pdb.set_trace()
				mapRequests['*'] = mapRequests['*']-1
	return (mapRequests, flag)

		
# when the RM receives a AM-cntr request, try to find any idle container to allocate		
def tryAlloAMCntr(currTime):
	global timeLastAllo
	cntrId = findOneIdleCntr()
	if cntrId != -1:
		jobId = RM.AMQueue.pop()
		if jobId in timeLastAllo:
			timeLastAllo[jobId] = currTime
			numCntrPerJob[jobId] = 1 # AM container is the first container allocated to a job
		# inform the job about its AM-container allocation
		AMCntr[jobId] = cntrId
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (currTime, jobId, 'am', 'start', cntrId) )
		RM.AMs[jobId].startAM.succeed()	
		
			

def findOneIdleCntr():
	global cntrStatus, SET_HOST, NUM_CNTR_PER_HOST
	# find an idle container in a randomly chosen host
	setHosts = SET_HOST[:]
	while setHosts != []:
		host = random.choice(setHosts)
		setHosts.remove(host)
		cntrPerHost = cntrStatus[host]
		if 0 in cntrPerHost:
			localIndex = cntrPerHost.index(0)
			cntrStatus[host][localIndex] = 1 # set the container status to busy
			# return cntrId
			return NUM_CNTR_PER_HOST*host + localIndex
	# cannot find an idle container
	return -1

		
# try to allocate idle contianers when receiving a message from a node manager	
def tryAlloCntrClusterQ(hostId, currTime):	
	rackId = MAPPING_HOST_TO_RACK[hostId]
	while 0 in cntrStatus[hostId]:		
		localCntrId = cntrStatus[hostId].index(0)
		numCntrPerJobSorted = sorted(numCntrPerJob.items(), key=itemgetter(1))
		jobId, taskType = allocateCntrClusterQ(numCntrPerJobSorted, rackId, currTime)
		if jobId != -1:
			if jobId in RM.clusterQueue and RM.clusterQueue[jobId] == {}:
				del RM.clusterQueue[jobId]
			numCntrPerJob[jobId] += 1
			cntrStatus[hostId][localCntrId] = 1
			cntrId = NUM_CNTR_PER_HOST*hostId + localCntrId
			RM.AMs[jobId].appMaster_proc.interrupt(['containerAllocation', [taskType, cntrId]]) 
		else: 
			return
	
					
			

















	
	
	
	
	
	
	
	
	
	
	
	
		
		
