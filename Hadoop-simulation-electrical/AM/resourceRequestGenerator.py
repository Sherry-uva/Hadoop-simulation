'''
Generate resource mapRequests for map containers
The map resource mapRequests will be sent to the YARN scheduler all at once when a job is submitted
'''

import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
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
'''
def generateMapResourceRequest(blockLocations, numBlocks, shuffleSize):
	mapRequests = {'*':numBlocks}
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
	priority1 = 'mR'
	priority2 = 'rR'
	requests[priority1] = mapRequests
	if shuffleSize == 0:
		return requests
	requests[priority2] = {'*':int(ceil(float(numBlocks)/8))}
	return requests
	

def generator(workload,trace):
	blockLocationAll = shelve.open('input/blockLocationAll_'+trace, flag='r')
	# pdb.set_trace()
	sOut = shelve.open('input/resourceRequestsAll_'+trace) # key: jobId, value: mapResourceRequest
	# workload = '/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/' + sys.argv[1]
	largeJobs = [line.strip() for line in open('input/largeJobs_'+trace+'.txt', 'r')]
	count = 0 
	with open(workload,'r') as csvfile :
			reader = csv.reader(csvfile)
			for job in reader:
				count += 1
				if count%1000 == 0:
					print '********************** ' + job[0] + ' **********************'
				if int(job[3]) > 0:
					if job[0] not in largeJobs:
						numBlocks = int(ceil(int(job[3])/(1024.0*1024*128)))
					else:
						numBlocks = int(ceil(int(job[3])/(1024.0**3)))
					blockLocations = blockLocationAll[job[6]][0:numBlocks]
					requests = generateMapResourceRequest(blockLocations, len(blockLocations), int(job[4]))
				else: # zero input dataset, estimate the number of mappers based on the shuffle size
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

	s = shelve.open('input/resourceRequestsAll_'+trace, flag='r')
	print s['job5']
	print s['job18']
	s.close()



























