import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
import simpy, csv
from random import randint

from YARN import resourceManager, containerAllocator
from globals import SIMLT_TIME, SET_HOST
import log as L 
from Network import electricalPaths


trace = 'numSHJ' + str(sys.argv[1]) + '_SHJRatio' + str(int(sys.argv[2])) + '%_arrivalRate' + str(sys.argv[3])
workload = '/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/SWIM/' + trace + '.csv'
L.logJobCompletionTime = 'logs/' + trace + '-jobCompletionTime-electrical' + str(sys.argv[4]) + '.csv'
L.logTasks = 'logs/' + trace + '-tasks-electrical' + str(sys.argv[4]) + '.csv'
L.logJobs = 'logs/' + trace + '-jobs-electrical' + str(sys.argv[4]) + '.csv'
L.largeJobs = [line.strip() for line in open('input/largeJobs_'+trace+'.txt', 'r')]
L.logCPUUtil = 'logs/' + trace + '-cpuUtil-electrical' + str(sys.argv[4]) + '.csv'

def jobArrival(env, pipeAMtoRM):
	count = 0
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
# 			if int(job[0][3:]) < 1:
# 				count += 1
#  				continue
			yield env.timeout(int(job[2]))
# 			print str(job[0]) + ' arrives at ' + str(env.now)
			pipeAMtoRM.put(simpy.PriorityItem(0, ['jobArrival', job[0], int(job[4])]))
			count += 1
# 			if count == 10:
# 				break
									
								
env = simpy.Environment()
# Communication pipe allowing AMs to talk to RM
pipe2RM = simpy.PriorityStore(env)
pipe2Epath = simpy.PriorityStore(env)
ePath = electricalPaths.ElectricalPath(env, pipe2Epath)
# start resourceManager process
RM = env.process(resourceManager.ResourceManager(env, pipe2RM, pipe2Epath, trace))
for host in SET_HOST:
	containerAllocator.NodeManager(env, pipe2RM, host)
jobArri = env.process(jobArrival(env, pipe2RM))

with open(L.logJobCompletionTime, 'wt') as f:
	writer = csv.writer(f)
	writer.writerow( ('jobId', 'numMaps', 'numReduces', 'startTime', 'finishTime', 'duration') )
with open(L.logTasks, 'wt') as f:
	writer = csv.writer(f)
	writer.writerow( ('time', 'jobId', 'taskId', 'event', 'cntrId') )
with open(L.logJobs, 'wt') as f:
	writer = csv.writer(f)
	writer.writerow( ('jobId', 'numMaps', 'numReduces') )
with open(L.logCPUUtil, 'wt') as f:
	writer = csv.writer(f)
	writer.writerow( ('hostId', 'time', 'CPUUtilization') )
	
	
env.run(until=sys.argv[5])




























