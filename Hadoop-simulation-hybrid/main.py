import sys, traceback, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
import simpy, csv
from random import randint
import logging

from YARN import resourceManager, containerAllocator
from globals import SIMLT_TIME, SET_HOST
import log as L
from Network import opticalCircuits, electricalPaths


trace = 'numSHJ' + str(sys.argv[1]) + '_SHJRatio' + str(int(sys.argv[2])) + '%_arrivalRate' + str(sys.argv[3])
workload = '/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/' + trace + '.csv'
L.logJobCompletionTime = 'logs/' + trace + '-jobCompletionTime-hybrid' + str(sys.argv[4]) + '.csv'
L.logTasks = 'logs/' + trace + '-tasks-hybrid' + str(sys.argv[4]) + '.csv'
L.logJobs = 'logs/' + trace + '-jobs-hybrid' + str(sys.argv[4]) + '.csv'
L.logOptical = 'logs/' + trace + '-optical' + str(sys.argv[4]) + '.csv'
L.logCPUUtil = 'logs/' + trace + '-cpuUtil-hybrid' + str(sys.argv[4]) + '.csv'
L.largeRJs = [line.strip() for line in open('input/largeRJ_'+trace+'.txt', 'r')]
L.largeSHJs = [line.strip() for line in open('input/largeSHJ_'+trace+'.txt', 'r')]

def jobArrival(env, pipeAMtoRM):
	count = 0
	with open(workload, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for job in reader:
# 			if int(job[0][3:]) < 1:
# 				count += 1
# 				continue
			yield env.timeout(int(job[2]))
# 			print str(job[0]) + ' arrives at ' + str(env.now)
			pipeAMtoRM.put(simpy.PriorityItem(0, ['jobArrival', job[0], int(job[4])]))
			count += 1
# 			if count == 20:
# 				break
									

logging.basicConfig(format='%(levelname)s:%(message)s', filename='hadoop.log', filemode='w', level=logging.DEBUG)
								
env = simpy.Environment()
# Communication pipe allowing AMs to talk to RM
pipe2RM = simpy.PriorityStore(env)
pipe2SDN = simpy.Store(env)
SDNCtrl = opticalCircuits.SDNController(env, pipe2SDN)
pipe2Epath = simpy.PriorityStore(env)
ePath = electricalPaths.ElectricalPath(env, pipe2Epath)
# start resourceManager process
RM = env.process(resourceManager.ResourceManager(env, pipe2RM, pipe2SDN, pipe2Epath, trace))
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
with open(L.logOptical, 'wt') as f:
	writer = csv.writer(f)
	writer.writerow( ('jobId', 'mapRackId', 'reduceRackId', 'time', 'event') )
with open(L.logCPUUtil, 'wt') as f:
	writer = csv.writer(f)
	writer.writerow( ('hostId', 'time', 'CPUUtilization') )


env.run(until=sys.argv[5])




























