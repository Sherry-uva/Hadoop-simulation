import random
import numpy as np
import log as L

# terasort, ranked-inverted-index, self-join, word-sequence-count, adjacency-list,
# inverted-index, term-vector,
# grep, wordcount, classification, histogram-movies, histogram-ratingss
JOB_MAP = [0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 0.65, 0.75, 0.8, 0.9]
MAP_EXECUTION_TIME = np.ones(12)*15
REDUCE_EXECUTION_TIME = np.ones(12)*25
# MAP_EXECUTION_TIME = range(12)
# REDUCE_EXECUTION_TIME = range(12)


def taskExecutionTime(jobId):
	tmp = random.random()
# 	print 'tmp: ' + str(tmp)
	if tmp > max(JOB_MAP):
		i = len(JOB_MAP)
	else:	
		i = 0
		while tmp >= JOB_MAP[i]:
			i += 1
	mapExecTime = MAP_EXECUTION_TIME[i-1]
	reduceExecTime = REDUCE_EXECUTION_TIME[i-1]
	if jobId not in L.largeRJs and jobId not in L.largeSHJs:
		return (mapExecTime, reduceExecTime)
	else:
		return (mapExecTime*7, reduceExecTime*7)
	
# mapExecTime, reduceExecTime = taskExecutionTime()
# print 'map: ' + str(mapExecTime) + ' and reduce: ' + str(reduceExecTime)
