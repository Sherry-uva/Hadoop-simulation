import sys, csv, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
from globals import SET_CNTR


totalTime = int(sys.argv[1])
trace = sys.argv[2]
print 'total time: ' + str(totalTime)
logTask = trace + '-tasks-hybrid' + str(sys.argv[3]) + '.csv'
logJob = trace + '-jobs-hybrid' + str(sys.argv[3]) + '.csv'
print logJob
logComplTime = trace + '-jobCompletionTime-hybrid' + str(sys.argv[3]) + '.csv'
logOptic = trace + '-optical' + str(sys.argv[3]) + '.csv'
outputCircuit = trace + '-circuit' + str(sys.argv[3]) + '.csv'
outputTask = trace + '-tasks' + str(sys.argv[3]) + '.csv'
outputNumTasks = trace + '-numTasks' + str(sys.argv[3]) + '.csv'
outputCPU = trace + '-cpuUtil' + str(sys.argv[3]) + '.csv'


tasks = {}
with open(logTask, 'r') as csvfile:
	csvreader = csv.reader(csvfile, delimiter=",")
	for line in csvreader:	
		if line[1] not in tasks:
			tasks[line[1]] = {line[2]:[line[4], line[3], line[0]]}
		elif line[2] not in tasks[line[1]]: 
			tasks[line[1]][line[2]] = [line[4], line[3], line[0]]
		else:
			tasks[line[1]][line[2]] = tasks[line[1]][line[2]] + [line[3], line[0]]

timeTasks = {}
reduceCntrUtil = {}
with open(logJob, 'r') as csvfile, open(outputTask, 'wt') as output:
	csvfile.readline()
	csvreader = csv.reader(csvfile, delimiter=",")
	writer = csv.writer(output)
	writer.writerow( ('jobId', 'taskType', 'cntrId', 'start', 'startTime', 'execute', 'execTime', 'finish', 'finishTime') )
	for line in csvreader:	
		jobId = line[0]
# 		print jobId
		numMaps = int(line[1])
		numReduces = int(line[2])
		writer.writerow( ([jobId[3:], 'am'] + tasks[jobId]['am']) )
		timeTasks[jobId] = [[tasks[jobId]['am'][2], tasks[jobId]['am'][4]]]
		for i in range(numMaps):
# 			print 'map' + str(i) + ': ' + ','.join(tasks[jobId]['m'+str(i)])
			writer.writerow( ([jobId[3:], 'm'] + tasks[jobId]['m'+str(i)]) )
			timeTasks[jobId].append([tasks[jobId]['m'+str(i)][2], tasks[jobId]['m'+str(i)][6]])
		reduceCntrUtil[jobId] = 0
		for i in range(numReduces):
# 			print 'reduce' + str(i) + ': ' + ','.join(tasks[jobId]['r'+str(i)])
			writer.writerow( ([jobId[3:], 'r'] + tasks[jobId]['r'+str(i)]) )
			timeTasks[jobId].append([tasks[jobId]['r'+str(i)][2], tasks[jobId]['r'+str(i)][6]])
			startTime = float(tasks[jobId]['r'+str(i)][2])
			execTime = float(tasks[jobId]['r'+str(i)][4])
			finishTime = float(tasks[jobId]['r'+str(i)][6])
			reduceCntrUtil[jobId] += (finishTime-execTime)/(finishTime-startTime)
		if numReduces > 0:
			reduceCntrUtil[jobId] = reduceCntrUtil[jobId]/numReduces


step = 1
time = 0.0
jobs = []
with open(logJob, 'r') as csvfile:
	csvfile.readline()
	csvreader = csv.reader(csvfile, delimiter=",")
	for line in csvreader:
		jobs.append(line[0])
jobStartFinish = {}
with open(logComplTime, 'r') as csvfile:
	csvfile.readline()
	csvreader = csv.reader(csvfile, delimiter=",")
	for line in csvreader:
		jobStartFinish[line[0]] = [line[3], line[4]]
		
numCntrs = float(len(SET_CNTR)) # total number of containers	
# pdb.set_trace()	
with open(outputNumTasks, 'wt') as output:
	writer = csv.writer(output)
	writer.writerow((jobs+['numJobs', 'fairness']))
	while time < totalTime:
		numTasks = []
		activeJobs = []
		numJobs = 0
		for job in jobs:	
			numCntr = 0
			for cntr in timeTasks[job]:
				if time >= float(cntr[0]) and time <= float(cntr[1]):
					numCntr += 1
			numTasks.append(numCntr)
			if time >= float(jobStartFinish[job][0]) and time <= float(jobStartFinish[job][1]):
				activeJobs.append(int(job[3:]))
				numJobs += 1
		numTasks.append(numJobs)
		fairness = 0
		if numJobs > 0:
			for jobId in activeJobs:
				fairness += (numTasks[jobId]/numCntrs - 1.0/numJobs)**2
		numTasks.append(fairness)
		writer.writerow((numTasks))
		time += step


requests = {}
with open(logOptic, 'r') as csvfile:
	csvfile.readline()
	csvreader = csv.reader(csvfile, delimiter=",")
	for line in csvreader:	
		if line[0] not in requests:
			requests[line[0]] = {(line[1],line[2]):[line[3]]}
		elif (line[1],line[2]) not in requests[line[0]]: 
			requests[line[0]][(line[1],line[2])] = [line[3]]
		else:
			requests[line[0]][(line[1],line[2])].append(line[3])

with open (outputCircuit, 'wt') as output:
	writer = csv.writer(output)
	for key, value in requests.iteritems():
		for pair, time in value.iteritems():
			if int(pair[0]) != int(pair[1]):
				writer.writerow( (key[3:], pair[0], pair[1], time[0], time[1]) )	
	
	
with open('/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/'+trace+'.csv', 'r') as csvfile, open (outputCPU, 'wt') as output:
	reader = csv.reader(csvfile)
	writer = csv.writer(output)
	writer.writerow( ('jobId', 'jobType', 'completionTime', 'cpuUtil') )
	for job in jobs:
		completionTime = float(jobStartFinish[job][1])-float(jobStartFinish[job][0])
		print job + ': ' + str("{0:.3f}".format(round(completionTime,3))) + '    ' + str("{0:.3f}".format(round(reduceCntrUtil[job],3)))
		line = next(reader)
		writer.writerow( (job[3:], line[1], str("{0:.3f}".format(round(completionTime,3))), str("{0:.3f}".format(round(reduceCntrUtil[job],3)))) )
			

	
	
	
	
	
	
	
	
	
	
	
