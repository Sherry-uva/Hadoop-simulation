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
		self.upOCs = []
		self.downOCs = []
		for rack in SET_RACK_N0:
			self.upOCs.append(np.zeros(AR_WINDOW))
			self.downOCs.append(np.zeros(AR_WINDOW))
		self.upOCS2EPS = []
		self.downOCS2EPS = []
		for i in range(NUM_LINK_OCS_EPS):
			self.upOCS2EPS.append(np.zeros(AR_WINDOW))
			self.downOCS2EPS.append(np.zeros(AR_WINDOW))
# 		self.oldTime = env.now
# 		self.currentTime = env.now
		# AR slot index of the current time
		self.oldSlotIndex = 0
		self.currentSlotIndex = 0
		self.controller_proc = env.process(self.controller(env, pipe2SDN))
		
	def controller(self, env, pipe2SDN):
		while True:
			msg = yield pipe2SDN.get()
			currTime = env.now
			self.currentSlotIndex = int(floor(currTime/AR_SLOT))
			if msg[0] == 'circuitRequest':
				with open(L.logOptical, 'a') as f:
					writer = csv.writer(f)
					for reduceRack in msg[4]:
						if msg[3] != reduceRack: # only if map and reduce racks are different, a circuit is needed
							writer.writerow( (msg[1], msg[3], reduceRack, env.now, 'request') )
				self.updateARWindows()
				# a new job
				if msg[1] not in self.circuitRequests:
					self.circuitRequests[msg[1]] = msg[2]
				# for each map-reduce rack pair, reserve a circuit, shuffle size is given in msg[5] (in bits)
				numSlots = int(ceil(float(msg[5])/(RATE_OCS*AR_SLOT)))
				print str(env.now) + ': Circuit request from ' + msg[1] + ', map rack ' + str(msg[3]) \
				+ ' and reduce rack(s) ' + ', '.join(map(str, msg[4])) \
				+ ', shuffle size is ' + str(msg[5]) + ' and shuffle duration is ' + str(numSlots) + ' slots'
				# single reduce-rack case
				if len(msg[4]) == 1:
					if msg[3] == msg[4][0]:
						env.process(self.circuit(env, pipe2SDN, msg[1], 0, msg[3], msg[4][0]))
					else:
						finishSlot = self.checkCircuitAvaiSingleRedRack(msg[3], msg[4][0], numSlots)
						# time interval between the current time and the time when the shuffle is complete		
						time2Finish = (self.currentSlotIndex+1)*AR_SLOT-currTime + (finishSlot-1)*AR_SLOT + float(msg[5])/RATE_OCS-(numSlots-1)*AR_SLOT
						env.process(self.circuit(env, pipe2SDN, msg[1], time2Finish, msg[3], msg[4][0]))
				else: # more than one reduce racks
					newReduceRacks = []
					for reduceRack in msg[4]:
						if msg[3] == reduceRack:
							env.process(self.circuit(env, pipe2SDN, msg[1], 0, msg[3], reduceRack))
						else:
							newReduceRacks.append(reduceRack)
					if len(newReduceRacks) == 1:
						finishSlot = self.checkCircuitAvaiSingleRedRack(msg[3], newReduceRacks[0], numSlots)
						time2Finish = (self.currentSlotIndex+1)*AR_SLOT-currTime + (finishSlot-1)*AR_SLOT + float(msg[5])/RATE_OCS-(numSlots-1)*AR_SLOT
						env.process(self.circuit(env, pipe2SDN, msg[1], time2Finish, msg[3], newReduceRacks[0]))
					if len(newReduceRacks) > 1:
					# finishSlots is a list of finish slots, one for each of the reduce racks
						finishSlots = self.checkCircuitAvai(msg[3], newReduceRacks, numSlots)
						count = 0
						for reduceRack in msg[4]:
							if msg[3] != reduceRack:
								time2Finish = (self.currentSlotIndex+1)*AR_SLOT-currTime + (finishSlots[count]-1)*AR_SLOT + float(msg[5])/RATE_OCS-(numSlots-1)*AR_SLOT
								env.process(self.circuit(env, pipe2SDN, msg[1], time2Finish, msg[3], reduceRack))
								count += 1
				continue
			if msg[0] == 'circuitFinish':
				with open(L.logOptical, 'a') as f:
					writer = csv.writer(f)
					writer.writerow( (msg[1], msg[2], msg[3], env.now, 'finish') )
				self.circuitRequests[msg[1]].interrupt(['partialShufCmplt', msg[2], msg[3]])
	
			
	def circuit(self, env, pipe2SDN, jobId, waitTime, mapRackId, reduceRackId):
		yield env.timeout(waitTime)
		pipe2SDN.put(['circuitFinish', jobId, mapRackId, reduceRackId])
	
	''' decide whether the storage needs to be involved
		first try to multicast the shuffle data from the map rack to all the reduce racks at the same time.
		If at least one of the reduce rack is not able to receiving the data through multicasting due to the 
		the unavailability of ToR2OCS and/or OCS2Core links, the shuffle data will be pushed to the storage from the map rack
	'''
	def checkCircuitAvai(self, mapRack, reduceRacks, numShuffleSlots):
		# find an OCS to core EPS link that can finish transmitting from the map rack to the core EPS most fast	
		finishSlotMap, indexOCS2EPS, slot2ReserveMap = self.findUpPath(mapRack, numShuffleSlots)
		self.reserveCircuit([mapRack, indexOCS2EPS], slot2ReserveMap, 0)
		# key: reduceRackId, value: finish slot
		reduceRackLinks = {}
		for rack in reduceRacks:
			# check if the ToR2OCS link of the reduce rack is available
			if (sum(np.logical_and(self.downOCs[rack], slot2ReserveMap)) != 0):
				continue
			else: 
				for index in range(NUM_LINK_OCS_EPS):
					if (sum(np.logical_and(self.downOCS2EPS[index], slot2ReserveMap)) == 0):
						# find the available EPS to OCS link
						reduceRackLinks[rack] = finishSlotMap
						self.reserveCircuit([rack, index], slot2ReserveMap, 1)
						break
				# after checking all the EPS to OCS links, if the rack still cannot find a feasible one, then the rest reduce racks won't either
				if rack not in reduceRackLinks:
					break
		# if not all reduce racks can find a feasible EPS to OCS link that matches the map-rack link, then storage is needed
		if len(reduceRackLinks) < len(reduceRacks):
			for rack in reduceRacks:
				# find a path for a reduce rack that needs pull down data from storage
				if rack not in reduceRackLinks:
					finishSlot, indexEPS2OCS, slot2ReserveReduce \
					= self.findDownPath(rack, numShuffleSlots, slot2ReserveMap)
					reduceRackLinks[rack] = finishSlot
					self.reserveCircuit([rack, indexEPS2OCS], slot2ReserveReduce, 1)
		return [reduceRackLinks[rack] for rack in reduceRacks]
	
		
	# linkType: upOCs, downOCs, upOCS2EPS, downOCS2EPS
	# flag = 0: up, flag = 1, down
	def reserveCircuit(self, linkIndices, slot2Reserve, flag):
		if flag == 0:
			if len(self.upOCs[linkIndices[0]]) != AR_WINDOW:
				pdb.set_trace()
			self.upOCs[linkIndices[0]] = np.logical_or(self.upOCs[linkIndices[0]], slot2Reserve)*1
			if len(linkIndices) == 2:
				self.upOCS2EPS[linkIndices[1]] = np.logical_or(self.upOCS2EPS[linkIndices[1]], slot2Reserve)*1
		else: 
			self.downOCs[linkIndices[0]] = np.logical_or(self.downOCs[linkIndices[0]], slot2Reserve)*1
			if len(linkIndices) == 2:
				self.downOCS2EPS[linkIndices[1]] = np.logical_or(self.downOCS2EPS[linkIndices[1]], slot2Reserve)*1	
				
		
	# find a OCS-to-core-EPS link for the map rack		
	def findUpPath(self, mapRack, numShuffleSlots):
		finishSlot = AR_WINDOW
		indexOCS2EPS = -1
		for index in range(NUM_LINK_OCS_EPS):
			slot2ReserveTmp, finishSlotTmp = self.findSlotToReserve(self.upOCs[mapRack], self.upOCS2EPS[index], numShuffleSlots)
			if finishSlotTmp == numShuffleSlots+1:
				slot2Reserve = slot2ReserveTmp
				finishSlot = finishSlotTmp
				indexOCS2EPS = index
				break
			if finishSlotTmp < finishSlot:
				slot2Reserve = slot2ReserveTmp
				finishSlot = finishSlotTmp
				indexOCS2EPS = index
		return (finishSlot, indexOCS2EPS, slot2Reserve)
	
	# find a core-EPS-to-OCS link for a map rack	
	def findDownPath(self, reduceRack, numShuffleSlots, slot2ReserveMap):
		finishSlot = AR_WINDOW
		indexEPS2OCS = -1
		for index in range(NUM_LINK_OCS_EPS):			
			slot2ReserveTmp, finishSlotTmp = self.findSlotToReservePullDown(self.downOCs[reduceRack], self.downOCS2EPS[index], numShuffleSlots, slot2ReserveMap)
			if finishSlotTmp < finishSlot:
				slot2Reserve = slot2ReserveTmp
				finishSlot = finishSlotTmp
				indexEPS2OCS = index
		return (finishSlot, indexEPS2OCS, slot2Reserve)
				
	# upon receiving a new circuit request, the AR windows of all the links will be updated
	# the start slot will correspond to the current slot		
	def updateARWindows(self):
# 		pdb.set_trace()
		removeSlots = self.currentSlotIndex - self.oldSlotIndex			
		self.oldSlotIndex = self.currentSlotIndex
		if removeSlots < AR_WINDOW:
			for rack in SET_RACK_N0:
				self.upOCs[rack] = np.delete(self.upOCs[rack], range(removeSlots))
				self.upOCs[rack] = np.append(self.upOCs[rack], np.zeros(removeSlots))
				self.downOCs[rack] = np.delete(self.downOCs[rack], range(removeSlots))
				self.downOCs[rack] = np.append(self.downOCs[rack], np.zeros(removeSlots))
			for i in range(NUM_LINK_OCS_EPS):
				self.upOCS2EPS[i] = np.delete(self.upOCS2EPS[i], range(removeSlots))
				self.upOCS2EPS[i] = np.append(self.upOCS2EPS[i], np.zeros(removeSlots))
				self.downOCS2EPS[i] = np.delete(self.downOCS2EPS[i], range(removeSlots))
				self.downOCS2EPS[i] = np.append(self.downOCS2EPS[i], np.zeros(removeSlots))
		else:
			for rack in SET_RACK_N0:
				self.upOCs[rack] = np.zeros(AR_WINDOW)
				self.downOCs[rack] = np.zeros(AR_WINDOW)
			for i in range(NUM_LINK_OCS_EPS):
				self.upOCS2EPS[i] = np.zeros(AR_WINDOW)
				self.downOCS2EPS[i] = np.zeros(AR_WINDOW)
		
	''' for shuffling between one map rack and a single reduce rack, there are two possible choices
		one is use a circuit directly through the OCS
		the other is to first push the shuffle data to the storage, and pull down by the reduce rack afterwards	
	'''	
	def checkCircuitAvaiSingleRedRack(self, mapRack, reduceRack, numShuffleSlots):
		slot2ReserveDirect, finishSlotDirect = self.findSlotToReserve(self.upOCs[mapRack], self.downOCs[reduceRack], numShuffleSlots)
		# if a direct circuit could be set up without any waiting, then use the direct circuit
		if finishSlotDirect == numShuffleSlots+1:
			self.reserveCircuit([mapRack], slot2ReserveDirect, 0)
			self.reserveCircuit([reduceRack], slot2ReserveDirect, 1)
			return finishSlotDirect
			
		# push shuffle data from the map rack to storage
		finishSlotStorageMap, indexOCS2EPS, slot2ReserveStorageMap = self.findUpPath(mapRack, numShuffleSlots)
		
		# pull down shuffle data to the reduce rack from storage
		finishSlotStorageReduce, indexEPS2OCS, slot2ReserveStorageReduce \
		= self.findDownPath(reduceRack, numShuffleSlots, slot2ReserveStorageMap)
		
		# use storage to facilitate the shuffle
		if finishSlotStorageReduce < finishSlotDirect:
			self.reserveCircuit([mapRack, indexOCS2EPS], slot2ReserveStorageMap, 0)
			self.reserveCircuit([reduceRack, indexEPS2OCS], slot2ReserveStorageReduce, 1)
			return finishSlotStorageReduce
		# use a direct circuit 
		else:
			self.reserveCircuit([mapRack], slot2ReserveDirect, 0)
			self.reserveCircuit([reduceRack], slot2ReserveDirect, 1)
			return finishSlotDirect
			

	# input: link availability along the reservation window, number of slots requested
	# output: slots that should be reserved on the links, finishSlot		
	def findSlotToReserve(self, statusLink1, statusLink2, numSlots):
		numLeftSlots = numSlots
		slot2Reserve = np.zeros(AR_WINDOW)
		# initialization, if the request cannot be satisfied within the reservation window, 
		# then the returned finishSlot is 0
		finishSlot = 0 
		# if an element in slotIdle is equal to 0, both links are idle in that slot
		slotIdle = np.logical_or(statusLink1, statusLink2)
		count = 0
		flag = 0 # to decide whether there are at least two consecutively available slots, 1: yes
		# circuit will start as soon as in the next slot (index 1)
		for i in range(1, AR_WINDOW):
			# counting the number of consecutive slots where both links are idle
			if slotIdle[i] == 0:
				count += 1
			else:
				# one slot will be used for reconfiguring circuit, which cannot transmit data
				if count >= 2:
					numLeftSlots -= count-1
					slot2Reserve[(i-count):i] = 1
				count = 0
			if count-1 == numLeftSlots:
				numLeftSlots = 0
				slot2Reserve[(i-count+1):(i+1)] = 1
				finishSlot = i # index of slot when the transmission is completed
				break
		return (slot2Reserve, finishSlot)	
	
	# the data pulled down from the storage should be at least one slot slower than it is been stored in the storage
	# link1: OCS-to-ToR link, link2: core-to-OCS link
	def findSlotToReservePullDown(self, statusLink1, statusLink2, numSlots, slot2ReservePush):	
		numLeftSlots = numSlots
		# data that could be pulled down from storage, meaning they have been pushed to the storage by the map rack
		numOkSlots = 0 
		slot2ReservePull = np.zeros(AR_WINDOW)
		# initialization, if the request cannot be satisfied within the reservation window, 
		# then the returned finishSlot is 0
		finishSlot = 0 
		slotIdle = np.logical_or(statusLink1, statusLink2)
		count = 0
		flag = 0 # to decide whether there are at least two consecutively available slots, 1: yes
		for i in range(1,AR_WINDOW):
			if slot2ReservePush[i] == 1 and slot2ReservePush[i-1] == 1:
				numOkSlots += 1
			# if there is no data in the storage, no need to consider the current slot
			if slot2ReservePush[i] == 0 and numOkSlots == 0:
				continue
			# counting the number of consecutive slots where both links are idle
			if slotIdle[i] == 0 and count <= numOkSlots:
				count += 1
			else:
				# one slot will be used for reconfiguring circuit, which cannot transmit data
				if count >= 2:
					numLeftSlots -= count-1
					numOkSlots -= count-1
					slot2ReservePull[(i-count):i] = 1
				count = 0
			if count-1 == numLeftSlots:
				numLeftSlots = 0
				slot2ReservePull[(i-count+1):(i+1)] = 1
				finishSlot = i # index of slot when the transmission is completed
				break
		return (slot2ReservePull, finishSlot)	
		
	


















	
