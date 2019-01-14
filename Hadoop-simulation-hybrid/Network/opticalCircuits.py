import simpy, pdb, csv
import numpy as np
from math import floor, ceil

from globals import SET_RACK_N0, NUM_LINK_OCS_EPS, AR_SLOT, AR_WINDOW, RATE_OCS
import log as L

class SDNController:
	
	def __init__(self, env, pipe2SDN):	
		# a dictionary storing circuit requests from AMs
		# key: jobId, value: AMProcess
		self.circuitRequests = {}
		self.controller_proc = env.process(self.controller(env, pipe2SDN))
		
	def controller(self, env, pipe2SDN):
		while True:
			msg = yield pipe2SDN.get()
			if msg[0] == 'circuitRequest':
				with open(L.logOptical, 'a') as f:
					writer = csv.writer(f)
					for reduceRack in msg[4]:
						if msg[3] != reduceRack: # only if map and reduce racks are different, a circuit is needed
							writer.writerow( (msg[1], msg[3], reduceRack, env.now, 'request') )				
				# a new job
				if msg[1] not in self.circuitRequests:
					self.circuitRequests[msg[1]] = msg[2]
				# for each map-reduce rack pair, reserve a circuit, shuffle size is given in msg[5] (in bits)
				numSlots = int(ceil(float(msg[5])/(RATE_OCS*AR_SLOT)))
				print str(env.now) + ': Circuit request from ' + msg[1] + ', map rack ' + str(msg[3]) \
				+ ' and reduce rack(s) ' + ', '.join(map(str, msg[4])) \
				+ ', shuffle size is ' + str(msg[5]) + ' and shuffle duration is ' + str(numSlots) + ' slots'
				for reduceRack in msg[4]:
					if msg[3] == reduceRack:
						env.process(self.circuit(env, pipe2SDN, msg[1], 0, msg[3], reduceRack))
					else:		
						env.process(self.circuit(env, pipe2SDN, msg[1], (numSlots+1)*AR_SLOT, msg[3], reduceRack))
				continue
			if msg[0] == 'circuitFinish':
				with open(L.logOptical, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (msg[1], msg[2], msg[3], env.now, 'finish') )
				self.circuitRequests[msg[1]].interrupt(['partialShufCmplt', msg[2], msg[3]])
	
			
	def circuit(self, env, pipe2SDN, jobId, waitTime, mapRackId, reduceRackId):
		yield env.timeout(waitTime)
		pipe2SDN.put(['circuitFinish', jobId, mapRackId, reduceRackId])
	












	
