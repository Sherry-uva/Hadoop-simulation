import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
import simpy

 
from globals import SET_RACK, RATE_INTRA_RACK, RATE_INTER_RACK, EPATH_UPDATE_INTERVAL

class ElectricalPath:
		
	def __init__(self, env, pipe2Epath):
		self.rateUp = [0 for i in SET_RACK]
		self.rateDown = [0 for i in SET_RACK]
		self.numFlowUp = [0 for i in SET_RACK]
		self.numFlowDown = [0 for i in SET_RACK]
		self.flows = []
		self.ePath_proc = env.process(self.ePath(env, pipe2Epath))
		self.update_proc = env.process(self.ePathUpdate(env, pipe2Epath))
		
	def ePathUpdate(self, env, pipe2Epath):
		while True:
			yield env.timeout(EPATH_UPDATE_INTERVAL)
			pipe2Epath.put(simpy.PriorityItem(1, ['update']))
			
	def ePath(self, env, pipe2Epath):
		while True:
			message = yield pipe2Epath.get()
			msg = message[1]
			# a new flow arrives
			if msg[0] == 'newFlow':
# 				pdb.set_trace()
				if msg[1] != msg[2]: # inter-rack flow
					self.numFlowUp[msg[1]] += 1
					self.numFlowDown[msg[2]] += 1
				self.flows.append(msg[1:])
				continue
			if msg[0] == 'update':
				unfinishedFlows = []
				for flow in self.flows:
					# flow has completed during the last tic
					if flow[2] <= 0:
						if flow[0] != flow[1]:
							self.numFlowUp[flow[0]] -= 1
							self.numFlowDown[flow[1]] -= 1
						flow[3].put(['flowFinish', flow[4]])
					else:
						unfinishedFlows.append(flow)
				self.flows[:] = unfinishedFlows
				for i in SET_RACK:
					if self.numFlowUp[i] > 0:
						self.rateUp[i] = float(RATE_INTER_RACK)/self.numFlowUp[i]							
					if self.numFlowDown[i] > 0:
						self.rateDown[i] = float(RATE_INTER_RACK)/self.numFlowDown[i]
				if self.flows != []:
					self.updateFlow()

	def updateFlow(self):
		for flow in self.flows:		
			if flow[0] != flow[1]: # inter-rack flow
				flowRate = min(self.rateUp[flow[0]], self.rateDown[flow[1]], RATE_INTRA_RACK)
				flow[2] -= flowRate*EPATH_UPDATE_INTERVAL
			else:
				flow[2] -= RATE_INTRA_RACK*EPATH_UPDATE_INTERVAL
		




			
		
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
