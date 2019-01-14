'''
Generate resource mapRequests for map containers
The map resource mapRequests will be sent to the YARN scheduler all at once when a job is submitted
'''

import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
import shelve, csv
from math import ceil

from globals import MAPPING_HOST_TO_RACK, NUM_REPLICAS
from input import inputSWIM


''' 
Generate resource requests for map containers for regular jobs 
and for shuffle-heavy jobs which are put into the Cluste queue
requests: dict(priority, dict(rackId, numContainers))
priority: 'mR', 'mSH'
rackId: 'r0', ..., 'r19', '*', 'R' -- set of racks storing this SHD
flag: 1 - SHJ, 0 - regular
setSHDRack: if flag == 1, then this parameter provides the set of rackst storing the SHD
'''
def generateMapResourceRequest(blockLocations, numBlocks, flag, setSHDRack, shuffleSize):
	mapRequests = {'*':numBlocks}
	if flag == 0: # regular jobs
		for i in range(numBlocks):
			racks = []
			for j in range(NUM_REPLICAS):
				rack = MAPPING_HOST_TO_RACK[blockLocations[i,j]]
				if rack not in mapRequests:
					mapRequests[rack] = 1
				else:
					if rack not in racks:
						mapRequests[rack] += 1
				racks.append(rack)
	requests = dict()
	if flag == 0:
		priority1 = 'mR'
		priority2 = 'rR'
	else:
		priority1 = 'mSH'
		priority2 = 'rSH'
		mapRequests['R'] = setSHDRack
	requests[priority1] = mapRequests
	if flag == 0 and shuffleSize == 0:
		return requests
	requests[priority2] = {'*':int(ceil(float(numBlocks)/8))}
	return requests
	

def generator(workload, trace):
	blockLocationAll = shelve.open('input/blockLocationAll_'+trace, flag='r')
	# pdb.set_trace()
	rackSetOfInputPath = shelve.open('input/rackSetOfInputPath_'+trace, flag='r')
	sOut = shelve.open('input/resourceRequestsAll_'+trace) # key: jobId, value: mapResourceRequest
	# workload = "/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/100jobs_SHJRatio5%_arrivalRate0.2.csv"
	largeRJs = [line.strip() for line in open('input/largeRJ_'+trace+'.txt', 'r')]	
	largeSHJs = [line.strip() for line in open('input/largeSHJ_'+trace+'.txt', 'r')]
	largeJobs = largeRJs + largeSHJs
	count = 0 
	with open(workload,'r') as csvfile :
			reader = csv.reader(csvfile)
			for job in reader:
	# 			print job[0]
	# 			if job[0] == 'job1361':
	# 				pdb.set_trace()
				count += 1
				if count%1000 == 0:
					print '********************** ' + job[0] + ' **********************'
				if int(job[3]) > 0:
					if job[0] not in largeJobs:
						numBlocks = int(ceil(int(job[3])/(1024.0*1024*128)))
					else:
						numBlocks = int(ceil(int(job[3])/(1024.0**3)))
					blockLocations = blockLocationAll[job[6]][0:numBlocks]
					if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000: # SHJ
						setSHDRack = rackSetOfInputPath[job[6]]
						requests = generateMapResourceRequest(blockLocations, len(blockLocations), 1, setSHDRack, int(job[4]))
					else:
						requests = generateMapResourceRequest(blockLocations, len(blockLocations), 0, [], int(job[4]))
				else: # zero input dataset, estimate the number of mappers based on the shuffle size
					if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000:
						priority1 = 'mSH'
						priority2 = 'rSH'
					else:
						priority1 = 'mR'
						priority2 = 'rR'
					if int(job[4]) > 0:
						numBlocks = int(ceil(int(job[4])/(1024.0*1024*256)))		
						requests = {priority1:{'*':numBlocks}}
						requests[priority2] = {'*':int(ceil(float(numBlocks)/8))}
					else: # zero shuffle data, so no reduce task
						requests = {priority1:{'*':1}}
					blockLocations = []
				sOut[job[0]] = [requests, blockLocations]
	sOut.close()
	blockLocationAll.close()
	rackSetOfInputPath.close()

	s = shelve.open('input/resourceRequestsAll_'+trace, flag='r')
	print s['job4']
	print s['job7']
	print s['job34']
	print s['job45']
	print s['job73']
	s.close()



























