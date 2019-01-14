import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
import numpy as np
import simpy, logging, csv
from math import ceil

from globals import MAPPING_HOST_TO_RACK, MAPPING_CNTR_TO_HOST, MAPPING_CNTR_TO_RACK, \
REDUCE_SLOW_START, REDUCE_RAMPUP_LIMIT, SIMLT_TIME, AM_INIT_TIME, NUM_REPLICAS
import log as L
import task, executionTime
from YARN import containerAllocator as CA


# Monitor the progress of a job
# Also responsible for submitting reduce-container requests
class AppMaster:
	global MAPPING_HOST_TO_RACK, MAPPING_CNTR_TO_HOST, MAPPING_CNTR_TO_RACK, \
	REDUCE_SLOW_START, REDUCE_RAMPUP_LIMIT, SIMLT_TIME, AM_INIT_TIME
	def __init__(self, env, pipeAMtoRM, pipe2SDN, pipe2Epath, jobId, resourceRequests, blockLocations, shuffleSize, flag):
		self.env = env
		self.jobId = jobId
		self.resourceRequests = resourceRequests
		if flag == 0: # RJ
			self.numMaps = resourceRequests['mR']['*']
			self.mapRequests = resourceRequests['mR']
			self.numReduces = resourceRequests['rR']['*']
			# in bits instead of bytes!
			self.shuffleSize = ceil(float(shuffleSize*8)/(self.numMaps*self.numReduces)) # per map per reduce shuffle size in bits		
			self.appMaster_proc = env.process(self.appMasterRJ(env, pipeAMtoRM, pipe2Epath))
			# communication pipes, one for each reduce, allowing maps to inform
			# reduces about their completeness
			self.pipes2Reduce = [simpy.Store(env, capacity = self.numMaps) for k in range(self.numReduces)]
			self.flagRedReq1 = 0
			self.flagRedReq2 = 0
		elif flag == 1: # SHJ
			self.mapRacks = resourceRequests['mSH']['R']
			self.numMaps = resourceRequests['mSH']['*']
			self.numReduces = resourceRequests['rSH']['*']
			self.mapReqPerRack = {}
			self.reduceReqPerRack = {}
			self.mapCompletedPerRack = {}
			self.reduceRacks = []
			self.delayedCircuitRequests = []
			self.appMaster_proc = env.process(self.appMasterSHJ(env, pipeAMtoRM, pipe2SDN, pipe2Epath))
			self.shuffleSize = shuffleSize # in bytes
			self.shuffleSizePerMap = ceil(float(shuffleSize*8)/self.numMaps) # per map shuffle size in bits
			self.shuffleSizePerReduce = ceil(float(shuffleSize*8)/self.numReduces) # per reduce shuffle size in bits
		else: # RJ with zero shuffle data, thus no reduce task
			self.numMaps = resourceRequests['mR']['*']
			self.mapRequests = resourceRequests['mR']
			self.numReduces = 0
			self.shuffleSize = 0
			self.appMaster_proc = env.process(self.appMasterRJNoReduce(env, pipeAMtoRM, pipe2Epath))	
			self.pipes2Reduce = []	
		self.startTime = env.now
# 		print blockLocations
		self.mapMatrix = self.generateMapMatrix(blockLocations)	
		# map-task status: 0 - pending, 1 - requested, 2 - assigned, 3 - movingInput, 4 - executing, 5 - completed
		self.mapStatus = [0 for k in range(self.numMaps)]
		# [cntrId, cntrRackId, inputRackId]
		self.mapCntr = [[-1, -1, -1] for k in range(self.numMaps)]
		# map-task processes
		self.mapTasks = {}
		# reduce-task status: 0 - pending, 1 - requested, 2 - assigned, 3 - shuffling, 4 - executing, 5 - completed
		self.reduceStatus = [0 for k in range(self.numReduces)]
		# [cntrId, cntrRackId]
		self.reduceCntr = [[-1, -1] for k in range(self.numReduces)]
		# reduce-task processes
		self.reduceTasks = {}
		self.mapExecTime, self.reduceExecTime = executionTime.taskExecutionTime(self.jobId)
		self.startAM = env.event()
	
	def appMasterRJ(self, env, pipeAMtoRM, pipe2Epath):
		with open(L.logJobs, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (self.jobId, self.numMaps, self.numReduces)) 
		pipeAMtoRM.put(simpy.PriorityItem(0, ['reqAMCntr', self.jobId, self.startAM]))
		# wait for a container being allocated to the AM
		yield self.startAM
		logging.info('%f -- Regular %s receives its AM container cntr%d', env.now, self.jobId, CA.AMCntr[self.jobId])
		print str(env.now) + ': Regular ' + self.jobId + ' receives its AM container cntr' + str(CA.AMCntr[self.jobId])
		print self.jobId
		print self.resourceRequests
		print self.mapMatrix
		print CA.cntrStatus
		yield env.timeout(AM_INIT_TIME)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'null', 'mapReq', self.numMaps) )
		pipeAMtoRM.put(simpy.PriorityItem(1, ['updatedResourceRequestRJ', self.jobId, {'mR':self.mapRequests}]))
		print str(env.now) + ': Regular ' + self.jobId + ' requests ' + str(self.numMaps) + ' map containers'
		self.mapStatus = [1 for k in range(self.numMaps)]
		numCompletedReduce = 0 # number of completed reduce tasks
		while numCompletedReduce < self.numReduces:
			try:
				yield env.timeout(SIMLT_TIME)
			except simpy.Interrupt as i: 
# 				print i.cause
				# receiving allocated containers from RM
				if i.cause[0] == 'containerAllocation':
# 					print 'Regular ' + self.jobId + ' receives a contianer allocation at time ' + str(env.now)
# 					print i.cause[1]
# 					if self.jobId == 'job6':
# 						pdb.set_trace()
					assignedMapId = self.containerAssignerRJ(i.cause[1], pipe2Epath, pipeAMtoRM)
				# a map task finishes
				if i.cause[0] == 'mapFinish':
					with open(L.logTasks, 'a') as f:
						writer = csv.writer(f)
						writer.writerow( (env.now, self.jobId, 'm'+str(i.cause[1]), 'finish', self.mapCntr[i.cause[1]][0]) )
					print str(env.now) + ': Regular ' + self.jobId + ' finishes map task ' + str(i.cause[1]) + ' which uses cntr' + str(self.mapCntr[i.cause[1]][0]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[self.mapCntr[i.cause[1]][0]])
					self.mapStatus[i.cause[1]] = 5
					# inform the RM about the finished container (cntrId: self.mapCntr[i.cause[1]][0])
					pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, self.mapCntr[i.cause[1]][0]]))
					assignedMapId = -1
				# a reduce task finishes
				if i.cause[0] == 'reduceFinish':
					with open(L.logTasks, 'a') as f:
						writer = csv.writer(f)
						writer.writerow( (env.now, self.jobId, 'r'+str(i.cause[1]), 'finish', self.reduceCntr[i.cause[1]][0]) )
					print str(env.now) + ': Regular ' + self.jobId + ' finishes reduce task ' + str(i.cause[1]) + ' which uses cntr' + str(self.reduceCntr[i.cause[1]][0]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[self.reduceCntr[i.cause[1]][0]])
					self.reduceStatus[i.cause[1]] = 5
					pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, self.reduceCntr[i.cause[1]][0]]))
					numCompletedReduce += 1
					if numCompletedReduce == self.numReduces:
						continue	
					assignedMapId = -1				
				numReqReduces = self.calculateNumReqReduceCntrs()
				updatedRR = self.updateResourceRequestRJ(numReqReduces, assignedMapId)
# 				print 'Regular ' + self.jobId + ': finished map/reduce tasks -- ' + str(self.mapStatus.count(5)) + ', ' + str(self.reduceStatus.count(5))
# 				print updatedRR
				if len(updatedRR) > 0:
					pipeAMtoRM.put(simpy.PriorityItem(1, ['updatedResourceRequestRJ', self.jobId, updatedRR]))
		finishTime = env.now
		while env.now - finishTime < 0.5:
			try: 
				yield env.timeout(1)
			except simpy.Interrupt as i:
				pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, i.cause[1][1]]))
		pipeAMtoRM.put(simpy.PriorityItem(2, ['jobCompletion', self.jobId]))
		
	def appMasterRJNoReduce(self, env, pipeAMtoRM, pipe2Epath):
		with open(L.logJobs, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (self.jobId, self.numMaps, self.numReduces)) 
		pipeAMtoRM.put(simpy.PriorityItem(0, ['reqAMCntr', self.jobId, self.startAM]))
		# wait for a container being allocated to the AM
		yield self.startAM
		logging.info('%f -- Regular %s with no reduces receives its AM container cntr%d', env.now, self.jobId, CA.AMCntr[self.jobId])
		print str(env.now) + ': Regular ' + self.jobId + ' receives its AM container cntr' + str(CA.AMCntr[self.jobId])
		print self.jobId
		print self.resourceRequests
		print self.mapMatrix
# 		print 'Input block locations: '
# 		print self.mapMatrix
		print CA.cntrStatus
		yield env.timeout(AM_INIT_TIME)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'null', 'mapReq', self.numMaps) )
		pipeAMtoRM.put(simpy.PriorityItem(1, ['updatedResourceRequestRJ', self.jobId, {'mR':self.mapRequests}]))
		print str(env.now) + ': Regular ' + self.jobId + ' requests ' + str(self.numMaps) + ' map containers'
		self.mapStatus = [1 for k in range(self.numMaps)]
		numCompletedMap = 0 # number of completed map tasks
		while numCompletedMap < self.numMaps:
			try:
				yield env.timeout(SIMLT_TIME)
			except simpy.Interrupt as i: 
# 				print i.cause
				# receiving allocated containers from RM
				if i.cause[0] == 'containerAllocation':
# 					print 'Regular ' + self.jobId + ' receives a contianer allocation at time ' + str(env.now)
# 					print i.cause[1]
# 					if self.jobId == 'job6':
# 						pdb.set_trace()
					assignedMapId = self.containerAssignerRJ(i.cause[1], pipe2Epath, pipeAMtoRM)
				# a map task finishes
				if i.cause[0] == 'mapFinish':
					with open(L.logTasks, 'a') as f:
						writer = csv.writer(f)
						writer.writerow( (env.now, self.jobId, 'm'+str(i.cause[1]), 'finish', self.mapCntr[i.cause[1]][0]) )
					print str(env.now) + ': Regular ' + self.jobId + ' finishes map task ' + str(i.cause[1]) + ' which uses cntr' + str(self.mapCntr[i.cause[1]][0]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[self.mapCntr[i.cause[1]][0]])
					self.mapStatus[i.cause[1]] = 5
					# inform the RM about the finished container (cntrId: self.mapCntr[i.cause[1]][0])
					pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, self.mapCntr[i.cause[1]][0]]))
					assignedMapId = -1	
					numCompletedMap += 1					
				updatedRR = self.updateResourceRequestRJ(0, assignedMapId)
# 				print 'Regular ' + self.jobId + ': finished map/reduce tasks -- ' + str(self.mapStatus.count(5)) + ', ' + str(self.reduceStatus.count(5))
# 				print updatedRR
				if len(updatedRR) > 0:
					pipeAMtoRM.put(simpy.PriorityItem(1, ['updatedResourceRequestRJ', self.jobId, updatedRR]))
		finishTime = env.now
		while env.now - finishTime < 0.5:
			try: 
				yield env.timeout(1)
			except simpy.Interrupt as i:
				pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, i.cause[1][1]]))
		pipeAMtoRM.put(simpy.PriorityItem(2, ['jobCompletion', self.jobId]))
	
	def appMasterSHJ(self, env, pipeAMtoRM, pipe2SDN, pipe2Epath):
		with open(L.logJobs, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (self.jobId, self.numMaps, self.numReduces)) 
		pipeAMtoRM.put(simpy.PriorityItem(0, ['reqAMCntr', self.jobId, self.startAM]))
		# wait for a container being allocated to the AM
		yield self.startAM
		logging.info('%f -- SHJ %s receives its AM container cntr%d', env.now, self.jobId, CA.AMCntr[self.jobId])
		print str(env.now) + ': SHJ ' + self.jobId + ' receives its AM container cntr' + str(CA.AMCntr[self.jobId])
		print self.jobId
		print self.resourceRequests
		print CA.cntrStatus
		yield env.timeout(AM_INIT_TIME)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'null', 'mapReq', self.numMaps) )
		pipeAMtoRM.put(simpy.PriorityItem(int(self.jobId[3:]), ['mapRequestSHJ', self.jobId, self.numMaps, self.mapRacks, self.shuffleSize]))
# 		print str(env.now) + ': SHJ ' + self.jobId + ' reqeusts ' + str(self.numMaps) + ' map containers on racks ' + ', '.join(map(str, self.mapRacks))
# 		print 'Input block locations: '
# 		print self.mapMatrix
		self.mapStatus = [1 for k in range(self.numMaps)] 
		numCompletedMap = 0 # number of completed map tasks
		numCompletedReduce = 0 # number of completed reduce tasks
		flagReduce = -1 # indicate whether reduce-container requests are sent or not
		while numCompletedReduce < self.numReduces:
			try:
				yield env.timeout(SIMLT_TIME)
			except simpy.Interrupt as i:
# 				print i.cause 	
				# receiving allocated containers from RM
				if i.cause[0] == 'containerAllocation':
# 					print 'SHJ ' + self.jobId + ' receives a container allocation with ' + str(len(i.cause[1])) + ' container(s) at time ' + str(env.now)
# 					print i.cause[1]
					self.containerAssignerSHJ(i.cause[1], pipe2Epath)
# 					print 'Finish one assigning... The number of assinged map tasks is ' + str(self.numMaps-self.mapStatus.count(1))
# 					print self.mapStatus
					continue
				# a map task finishes
				if i.cause[0] == 'mapFinish':
					with open(L.logTasks, 'a') as f:
						writer = csv.writer(f)
						writer.writerow( (env.now, self.jobId, 'm'+str(i.cause[1]), 'finish', self.mapCntr[i.cause[1]][0]) )
					print str(env.now) + ': SHJ ' + self.jobId + ' finishes map task ' + str(i.cause[1]) + ' which uses cntr' + str(self.mapCntr[i.cause[1]][0]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[self.mapCntr[i.cause[1]][0]])
					self.mapStatus[i.cause[1]] = 5
					pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, self.mapCntr[i.cause[1]][0]]))
					rackId = self.mapCntr[i.cause[1]][1]
					self.mapCompletedPerRack[rackId] += 1
					if self.mapCompletedPerRack[rackId] == self.mapReqPerRack[rackId]:
						# if all the map tasks on one map rack have completed for the first time, then send reduce-container requests
						if flagReduce == -1:
							flagReduce = rackId	
							print str(env.now) + ': SHJ ' + self.jobId + ' finishes all the map tasks on rack ' + str(rackId) + ' and asks the RM for reduce racks'
							pipeAMtoRM.put(simpy.PriorityItem(2, ['reduceRequestSHJ', self.jobId, self.numReduces]))
						# if the following map rack(s) finishes all its maps, request for circuit(s)
						else: 
							print str(env.now) + ': SHJ ' + self.jobId + ' finishes all the map tasks on rack ' + str(rackId)
							if self.reduceRacks != []:
								print str(env.now) + ': SHJ ' + self.jobId + ' requests circuit(s) between map rack ' + str(rackId) + ' and reduce rack(s) ' + ', '.join(map(str, self.reduceRacks)) 
								pipe2SDN.put(['circuitRequest', self.jobId, self.appMaster_proc, rackId, self.reduceRacks, self.shuffleSizePerMap*self.mapCompletedPerRack[rackId]])
							else:
								self.delayedCircuitRequests.append([rackId, self.shuffleSizePerMap*self.mapCompletedPerRack[rackId]])
					continue		
				# a reduce task finishes
				if i.cause[0] == 'reduceFinish':
					with open(L.logTasks, 'a') as f:
						writer = csv.writer(f)
						writer.writerow( (env.now, self.jobId, 'r'+str(i.cause[1]), 'finish', self.mapCntr[i.cause[1]][0]) )
					print str(env.now) + ': SHJ ' + self.jobId + ' finishes reduce task ' + str(i.cause[1]) + ' which uses cntr' + str(self.reduceCntr[i.cause[1]][0]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[self.reduceCntr[i.cause[1]][0]])
					self.reduceStatus[i.cause[1]] = 5
					pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, self.reduceCntr[i.cause[1]][0]]))
					numCompletedReduce += 1
# 					print 'Number of completed reduce tasks: ' + str(numCompletedReduce) + ' at time ' + str(env.now)
					continue
				# number of map tasks per rack
				if i.cause[0] == 'mapReqPerRack':
# 					pdb.set_trace()
					self.mapRacks = [i.cause[1][k][0] for k in range(len(i.cause[1]))]
					tmp = [i.cause[1][k][1] for k in range(len(i.cause[1]))]
					print str(env.now) + ': SHJ ' + self.jobId + ' has selected map racks ' + ', '.join(map(str, self.mapRacks)) + ' and number of map containers requested on each map rack is ' + ', '.join(map(str, tmp))
					print CA.cntrStatus
					self.mapReqPerRack = {i.cause[1][k][0]:i.cause[1][k][1] for k in range(len(i.cause[1]))}
					self.mapCompletedPerRack = {i.cause[1][k][0]:0 for k in range(len(i.cause[1]))}
					continue
				# when the first map rack finishes all its maps, reduce-container requests are sent to the RM,
				# which returns the set of reduce racks. Then the AM requests circuits from the SDN controller 
				if i.cause[0] == 'reduceRacks':
					self.reduceStatus = [1 for k in range(self.numReduces)]
					self.reduceRacks = [i.cause[1][k][0] for k in range(len(i.cause[1]))]
					self.reduceReqPerRack = {i.cause[1][k][0]:i.cause[1][k][1] for k in range(len(i.cause[1]))}
					print str(env.now) + ': SHJ ' + self.jobId + ' receives its reduce rack(s) from the RM, rack ' + ', '.join(map(str, self.reduceRacks))
					shuffleMatrix = {k:[0 for j in self.mapRacks] for k in self.reduceRacks}
					# flagReduce stores the rackId of the first finished map rack
					# the last parameter is the size of map output generated on the first map rack
					print str(env.now) + ': SHJ ' + self.jobId + ' requests circuit(s) between map rack ' + str(flagReduce) + ' and reduce rack(s) ' + ', '.join(map(str, self.reduceRacks)) 
					pipe2SDN.put(['circuitRequest', self.jobId, self.appMaster_proc, flagReduce, self.reduceRacks, self.shuffleSizePerMap*self.mapCompletedPerRack[flagReduce]])
					for request in self.delayedCircuitRequests:
						print str(env.now) + ': SHJ ' + self.jobId + ' requests circuit(s) between map rack ' + str(request[0]) + ' and reduce rack(s) ' + ', '.join(map(str, self.reduceRacks)) 
						pipe2SDN.put(['circuitRequest', self.jobId, self.appMaster_proc, request[0], self.reduceRacks, request[1]])
					continue
				# shuffle from one map rack to one reduce rack is completed
				if i.cause[0] == 'partialShufCmplt':
					shuffleMatrix[i.cause[2]][self.mapRacks.index(i.cause[1])] = 1
					print str(env.now) + ': SHJ ' + self.jobId + ' map output is shuffled from map rack ' + str(i.cause[1]) + ' to reduce rack ' + str(i.cause[2])
					# all the map racks have shuffled their output to reduce rack 'i.cause[2]'
					if 0 not in shuffleMatrix[i.cause[2]]:
						with open(L.logTasks, 'a') as f:
							writer = csv.writer(f)
							writer.writerow( (env.now, self.jobId, 'null', 'reduceRackReq', self.reduceReqPerRack[i.cause[2]]) )
						print str(env.now) + ': SHJ ' + self.jobId + ' map output is finished shuffling to reduce rack ' + str(i.cause[2])
						pipeAMtoRM.put(simpy.PriorityItem(2, ['updateReduceRequestSHJ', self.jobId, i.cause[2]]))
						del shuffleMatrix[i.cause[2]] 
					continue				
		pipeAMtoRM.put(simpy.PriorityItem(2, ['jobCompletion', self.jobId]))
		pipe2SDN.put(['jobCompletion', self.jobId])
		
	
	# mapMatrix: numBlocksx6 matrix, first three columns are blockLocations, 
	# 4-6th columns are rack indices	
	def generateMapMatrix(self, blockLocations):
		mapMatrix = [blockLocations[i].tolist() + [0 for i in range(len(blockLocations[0])+1)] for i in range(len(blockLocations))]
		for i in range(len(mapMatrix)):
			for j in range(len(blockLocations[0])):
				mapMatrix[i][j+len(blockLocations[0])] = MAPPING_HOST_TO_RACK[blockLocations[i,j]]
		return mapMatrix

	def calculateNumReqReduceCntrs(self): 
		# Not yet reached the point to request for reduce containers
		numCompletedMaps = self.mapStatus.count(5)
		if numCompletedMaps < ceil(len(self.mapStatus)*REDUCE_SLOW_START):
			return 0
		elif numCompletedMaps < len(self.mapStatus): 
			tmp = int(REDUCE_RAMPUP_LIMIT*len(self.reduceStatus))
			numAlloReduces = sum(i >= 2 for i in self.reduceStatus)
			if self.flagRedReq1 == 0:
				self.flagRedReq1 = 1
				with open(L.logTasks, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (self.env.now, self.jobId, 'null', 'firstRedReq', tmp) )
				if tmp > 0:
					logging.info('%f -- Regular %s starts requesting %d out of %d reduce containers when %d out of %d map tasks are completed', self.env.now, self.jobId, tmp, self.numReduces, numCompletedMaps, self.numMaps)
					print str(self.env.now) + ': Regular ' + self.jobId + ' requests reduce containers for the first time, requesting ' + str(tmp) + ' containers'
			# update status of reduce tasks
			for i in range(tmp):
				if self.reduceStatus[i] == 0:
					self.reduceStatus[i] = 1
			return tmp-numAlloReduces
		else: # all map tasks are completed
			numAlloReduces = sum(i >= 2 for i in self.reduceStatus)
			if self.flagRedReq2 == 0:
				self.flagRedReq2 = 1
				with open(L.logTasks, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (self.env.now, self.jobId, 'null', 'allRedReq', self.numReduces) )
				print str(self.env.now) + ': Regular ' + self.jobId + ' finishes all the map tasks and requests for ' + str(len(self.reduceStatus)-numAlloReduces) + ' reduce containers'
			# update status of reduce tasks
			self.reduceStatus = [1 if x==0 else x for x in self.reduceStatus]
			return len(self.reduceStatus)-numAlloReduces
	
	def updateResourceRequestRJ(self, numReqReduces, assignedMapId):
		if assignedMapId != -1: # a new container is assigned to a map task with assignedMapId
			if self.mapRequests['*'] > 1:
				self.mapRequests['*'] -= 1
				if len(self.mapMatrix) > 0:
					racks = []
					for j in range(NUM_REPLICAS, NUM_REPLICAS*2):
						rack = self.mapMatrix[assignedMapId][j]
						if rack not in racks:
							if self.mapRequests[rack] > 1:
								self.mapRequests[rack] -= 1
							else:	
								del self.mapRequests[rack]
						racks.append(rack)
			else:
				self.mapRequests = {}
		requests = dict()
		if len(self.mapRequests) > 0:	
			requests['mR'] = self.mapRequests
		if numReqReduces > 0:
			requests['rR'] = {'*':numReqReduces}
		return requests
		
	def containerAssignerRJ(self, allocatedCntr, pipe2Epath, pipeAMtoRM):
		# a map container
		if allocatedCntr[0] == 'm':
			mapTaskId, cntrRackId, inputRackId = self.assignMapCntr(allocatedCntr[1])
			if mapTaskId == -1:
# 				pdb.set_trace()
				pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, allocatedCntr[1]]))
				return -1
			with open(L.logTasks, 'a') as f:
				writer = csv.writer(f)
				writer.writerow( (self.env.now, self.jobId, 'm'+str(mapTaskId), 'start', allocatedCntr[1]) )
			print str(self.env.now) + ': Regular ' + self.jobId + ' has a container cntr' + str(allocatedCntr[1]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[allocatedCntr[1]]) + ' assigned to map task ' + str(mapTaskId)
			if self.jobId in L.largeRJs:
				self.mapTasks[mapTaskId] = task.MapRJ(self.env, self.jobId, mapTaskId, inputRackId, cntrRackId, self.mapExecTime, self.shuffleSize, self.appMaster_proc, self.pipes2Reduce, pipe2Epath, 8)
			elif len(self.mapMatrix) > 0: # non-zero map input
				self.mapTasks[mapTaskId] = task.MapRJ(self.env, self.jobId, mapTaskId, inputRackId, cntrRackId, self.mapExecTime, self.shuffleSize, self.appMaster_proc, self.pipes2Reduce, pipe2Epath, 1)
			else:
				self.mapTasks[mapTaskId] = task.MapRJ(self.env, self.jobId, mapTaskId, inputRackId, cntrRackId, self.mapExecTime, self.shuffleSize, self.appMaster_proc, self.pipes2Reduce, pipe2Epath, 0)
			return mapTaskId
		# a reduce container
		else:
			reduceTaskId, cntrRackId = self.assignReduceCntr(allocatedCntr[1])
			if reduceTaskId == -1:
# 				pdb.set_trace()
				pipeAMtoRM.put(simpy.PriorityItem(2, ['cntrFinish', self.jobId, allocatedCntr[1]]))
				return -1 
# 			if self.jobId == 'job13' and reduceTaskId == 10:
# 				pdb.set_trace()
			with open(L.logTasks, 'a') as f:
				writer = csv.writer(f)
				writer.writerow( (self.env.now, self.jobId, 'r'+str(reduceTaskId), 'start', allocatedCntr[1]) )
			print str(self.env.now) + ': Regular ' + self.jobId + ' has a container cntr' + str(allocatedCntr[1]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[allocatedCntr[1]]) + ' assigned to reduce task ' + str(reduceTaskId)
			self.reduceTasks[reduceTaskId] = task.ReduceRJ(self.env, self.jobId, reduceTaskId, cntrRackId, self.reduceExecTime, self.numMaps, self.appMaster_proc, self.pipes2Reduce[reduceTaskId], pipe2Epath)
			return -1
				
	def containerAssignerSHJ(self, allocatedCntrs, pipe2Epath):
		for allocatedCntr in allocatedCntrs:
			# a map container
			if allocatedCntr[0] == 'm':
				mapTaskId, cntrRackId, inputRackId = self.assignMapCntr(allocatedCntr[1])
				with open(L.logTasks, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (self.env.now, self.jobId, 'm'+str(mapTaskId), 'start', allocatedCntr[1]) )
				if self.jobId not in L.largeSHJs:
					self.mapTasks[mapTaskId] = task.MapSHJ(self.env, self.jobId, mapTaskId, inputRackId, cntrRackId, self.mapExecTime, self.appMaster_proc, pipe2Epath, 1)
				else:
					self.mapTasks[mapTaskId] = task.MapSHJ(self.env, self.jobId, mapTaskId, inputRackId, cntrRackId, self.mapExecTime, self.appMaster_proc, pipe2Epath, 8)
				print str(self.env.now) + ': SHJ ' + self.jobId + ' has a container cntr' + str(allocatedCntr[1]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[allocatedCntr[1]]) + ' assigned to map task ' + str(mapTaskId)
			# a reduce container
			# for a SHJ, any allocated reduce containers should be either node-local or rack-local
			else:
				reduceTaskId, cntrRackId = self.assignReduceCntr(allocatedCntr[1])
				with open(L.logTasks, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (self.env.now, self.jobId, 'r'+str(reduceTaskId), 'start', allocatedCntr[1]) )
				self.reduceTasks[reduceTaskId] = task.ReduceSHJ(self.env, self.jobId, reduceTaskId, cntrRackId, self.reduceExecTime, self.shuffleSizePerReduce, self.appMaster_proc, pipe2Epath)
				print str(self.env.now) + ': SHJ ' + self.jobId + ' has a container cntr' + str(allocatedCntr[1]) + ' on rack ' + str(MAPPING_CNTR_TO_RACK[allocatedCntr[1]]) + ' assigned to reduce task ' + str(reduceTaskId)
				
				
	def assignMapCntr(self, cntrId):
		hostId = MAPPING_CNTR_TO_HOST[cntrId]
		rackId = MAPPING_HOST_TO_RACK[hostId]
		if len(self.mapMatrix) == 0: # zero map input
			for i in range(len(self.mapStatus)):
				if self.mapStatus[i] == 1:
					self.mapStatus[i] = 2
					self.mapCntr[i] = [cntrId, rackId, -1]
					return (i, rackId, rackId)
			return (-1, -1, -1)
		# try to assign the container to a map task whose input is node-local
		for i in range(len(self.mapStatus)):
			if self.mapStatus[i] == 1:
				if hostId in [self.mapMatrix[i][j] for j in range(NUM_REPLICAS)]:
					self.mapStatus[i] = 2
					self.mapMatrix[i][NUM_REPLICAS*2] = 1
					self.mapCntr[i] = [cntrId, rackId, rackId]
					return (i, rackId, rackId)
		# try to assign the container to a map task whose input is rack-local
		for i in range(len(self.mapStatus)):
			if self.mapStatus[i] == 1:
				if rackId in [self.mapMatrix[i][j] for j in range(NUM_REPLICAS, NUM_REPLICAS*2)]:
					self.mapStatus[i] = 2
					self.mapMatrix[i][NUM_REPLICAS*2] = 1
					self.mapCntr[i] = [cntrId, rackId, rackId]
					return (i, rackId, rackId)
		# assign a no-local container
		for i in range(len(self.mapStatus)):
# 			pdb.set_trace()
			if self.mapStatus[i] == 1:
				print 'Allocating a no-local container... cntr' + str(cntrId)
				self.mapStatus[i] = 2
				self.mapMatrix[i][NUM_REPLICAS*2] = 1
				self.mapCntr[i] = [cntrId, rackId, self.mapMatrix[i][NUM_REPLICAS]]
				return (i, rackId, self.mapMatrix[i][NUM_REPLICAS])	
		return (-1, -1, -1)
	
	def assignReduceCntr(self, cntrId):
		rackId = MAPPING_CNTR_TO_RACK[cntrId]
		i = 0
		while i < self.numReduces and self.reduceStatus[i] > 1:
			i += 1
		if (i < self.numReduces and self.reduceStatus[i] == 0) or i == self.numReduces:
			return (-1, -1)
		self.reduceStatus[i] = 2
		self.reduceCntr[i] = [cntrId, rackId]
		return (i, rackId)
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	