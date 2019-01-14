import simpy, pdb, csv
from globals import HDFS_BLOCK_SIZE, RATE_INTRA_RACK
import log as L
            
        
class MapRJ:
	def __init__(self, env, jobId, taskId, inputRackId, cntrRackId, mapExecTime, shuffleSize, appMaster, pipes2Reduce, pipe2Epath, blockSize):
		self.size = blockSize*HDFS_BLOCK_SIZE
		self.jobId = jobId
		self.taskId = taskId
		self.inputRackId = inputRackId
		self.cntrRackId = cntrRackId
		self.mapExecTime = mapExecTime
		self.shuffleSize = shuffleSize # per reduce shuffle size, assuming all the reduces have the same shuffle size
		self.appMaster = appMaster
		self.moveInput = simpy.Store(env)
		self.map_proc = env.process(self.map(env, pipes2Reduce, pipe2Epath))
		
	def map(self, env, pipes2Reduce, pipe2Epath):
		# start a flow to move map input if needed and wait for completion
		if self.size > 0 and self.inputRackId != self.cntrRackId:
			pipe2Epath.put(simpy.PriorityItem(0, ['newFlow', self.inputRackId, self.cntrRackId, self.size, self.moveInput, self.taskId]))
			yield self.moveInput.get()
			print str(env.now) + ': finishing moving map output from a different rack'
		else:
			yield env.timeout(self.size/RATE_INTRA_RACK)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'm'+str(self.taskId), 'execute') )			
		# map execution
		yield env.timeout(self.mapExecTime)
		# map finishes -- inform all the reduces about its completeness; interrupt AM
		for pipe in pipes2Reduce:
			# info. sent to each reduce: taskId of this map, rackId of the map output, size of the map output
			pipe.put(['mapFinish', self.taskId, self.cntrRackId, self.shuffleSize])
		# inform the AM about its completion
		self.appMaster.interrupt(['mapFinish', self.taskId])
		
				
class ReduceRJ:
	def __init__(self, env, jobId, taskId, cntrRackId, reduceExecTime, numMap, appMaster, pipe2Reduce, pipe2Epath):
		self.jobId = jobId
		self.taskId = taskId
		self.cntrRackId = cntrRackId
		self.reduceExecTime = reduceExecTime
		self.numMap = numMap
		self.appMaster = appMaster
		# number of completed shuffle flows 
		self.numFinishFlow = 0
		self.numFinishMaps = 0
		self.reduce_proc = env.process(self.reduce(env, pipe2Reduce, pipe2Epath))
		
	def reduce(self, env, pipe2Reduce, pipe2Epath):
		while self.numFinishFlow < self.numMap:
			msg = yield pipe2Reduce.get()
			# a message sent from a completed map task
			if msg[0] == 'mapFinish':
				self.numFinishMaps += 1
# 				print str(self.numFinishMaps) + ': reduce task ' + str(self.taskId) + ' receives a message from map task ' + str(msg[1]) + ' at time ' + str(env.now)
				if msg[3] > 0:
# 					print str(env.now) + ': shuffling from map ' + str(msg[1]) + ' on rack ' + str(msg[2]) + ' to reduce ' + str(self.taskId) + ' on rack ' + str(self.cntrRackId) + ' starts'
					pipe2Epath.put(simpy.PriorityItem(0, ['newFlow', msg[2], self.cntrRackId, msg[3], pipe2Reduce, msg[1]]))
				else:
					self.numFinishFlow += 1			
			# a message sent from a complete shuffle flow, msg[0] == 'flowFinish'
			else:
# 				print str(env.now) + ': shuffling from map ' + str(msg[1]) + ' to reduce ' + str(self.taskId) + ' finishes'
				self.numFinishFlow += 1	
# 				print str(self.numFinishFlow) + ': data is shuffled from map task ' + str(msg[1]) + ' to reduce task ' + str(self.taskId)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'r'+str(self.taskId), 'execute') )
		# all shuffle flows are finished, start reduce execution
		print str(env.now) + ': reduce task ' + str(self.taskId) + ' of ' + self.jobId + ' starts executing'	
		yield env.timeout(self.reduceExecTime)
		self.appMaster.interrupt(['reduceFinish', self.taskId])
		
class MapSHJ:
	def __init__(self, env, jobId, taskId, inputRackId, cntrRackId, mapExecTime, appMaster, pipe2Epath, blockSize):
		self.size = blockSize*HDFS_BLOCK_SIZE
		self.jobId = jobId
		self.taskId = taskId
		self.inputRackId = inputRackId
		self.cntrRackId = cntrRackId
		self.mapExecTime = mapExecTime
		self.appMaster = appMaster
		self.moveInput = simpy.Store(env)
		self.map_proc = env.process(self.map(env, pipe2Epath))
		
	def map(self, env, pipe2Epath):
		# start a flow to move map input if needed and wait for completion
		if self.size > 0 and self.inputRackId != self.cntrRackId:
			pipe2Epath.put(simpy.PriorityItem(0, ['newFlow', self.inputRackId, self.cntrRackId, self.size, self.moveInput]))
			yield self.moveInput.get()
		else:
			yield env.timeout(self.size/RATE_INTRA_RACK)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'm'+str(self.taskId), 'execute') )	
		# map execution
		yield env.timeout(self.mapExecTime)
		# map finishes -- interrupt AM
		self.appMaster.interrupt(['mapFinish', self.taskId])

class ReduceSHJ:
	def __init__(self, env, jobId, taskId, cntrRackId, reduceExecTime, reduceInputSize, appMaster, pipe2Epath):
		self.size = reduceInputSize
		self.jobId = jobId
		self.taskId = taskId
		self.inputRackId = cntrRackId
		self.cntrRackId = cntrRackId
		self.reduceExecTime = reduceExecTime
		self.appMaster = appMaster
		self.moveReduceInput = simpy.Store(env)
		self.reduce_proc = env.process(self.reduce(env, pipe2Epath))
		
	def reduce(self, env, pipe2Epath):
		# start a flow to move reduce input if needed and wait for completion
		if self.size > 0 and self.inputRackId != self.cntrRackId:
			pipe2Epath.put(simpy.PriorityItem(0, ['newFlow', self.inputRackId, self.cntrRackId, self.size, self.moveReduceInput]))
			yield self.moveReduceInput.get()
		else:
			yield env.timeout(self.size/RATE_INTRA_RACK)
		with open(L.logTasks, 'a') as f:
			writer = csv.writer(f)
			writer.writerow( (env.now, self.jobId, 'r'+str(self.taskId), 'execute') )
		# reduce execution
		yield env.timeout(self.reduceExecTime)
		self.appMaster.interrupt(['reduceFinish', self.taskId])
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
