from math import ceil,floor
import csv
import matplotlib.pyplot as plt
import pickle

workload = "/Users/sherry/Box Sync/Hadoop-simulation/input/SWIM/FB-2010_samples_24_times_1hr_withInputPaths_0.tsv"

def inputShuffleSizes(workload):
	SHD1 = []
	SHD2 = []
	count = 0
	with open(workload,'r') as tsvfile:
		with open("SHD1.txt",'a') as outputfile1, open ("SHD2.txt", 'a') as outputfile2:
			tsvreader = csv.reader(tsvfile, delimiter="\t")
			for line in tsvreader:
				if int(line[4]) >= int(line[3]) and int(line[4]) >= 1024*1024*1024:
					SHD1.append(line[0])
					outputfile1.write(",".join([item for item in line]))
					outputfile1.write('\n')
				if int(line[4]) >= 1024*1024*1024:
					SHD2.append(line[0])
					outputfile2.write(",".join([item for item in line]))
					outputfile2.write('\n')
				count += 1
				if count == 6000: 
					print count
					print len(SHD1)
					print len(SHD2)
					return
	
# 	print set(SHD2) - set(SHD1)
		
def interArrivailTime(workload):
	shuffleHeavyJobs = []
	with open(workload,'r') as tsvfile:
		tsvreader = csv.reader(tsvfile, delimiter="\t")
		for line in tsvreader:
			if int(line[4]) >= int(line[3]) and int(line[4]) >= 1024*1024*1024:
				shuffleHeavyJobs.append([line[0]] + [int(line[i]) for i in range(1,6)])
	shuffleHeavyJobs[0][2] = shuffleHeavyJobs[0][1]
	shuffleHeavyJobs[0].append(int(ceil(shuffleHeavyJobs[0][3]/(1024.0*1024*128))))
	interArrivals = [shuffleHeavyJobs[0][2]]
	for i in range(1,len(shuffleHeavyJobs)):
		shuffleHeavyJobs[i][2] = int(shuffleHeavyJobs[i][1]) - int(shuffleHeavyJobs[i-1][1])
		interArrivals.append(shuffleHeavyJobs[i][2])
		shuffleHeavyJobs[i].append(int(ceil(shuffleHeavyJobs[i][3]/(1024.0*1024*128))))
	f = open("shuffleHeavyJobs.txt", "w")
	f.write("\n".join([",".join([str(n) for n in item]) for item in shuffleHeavyJobs]))
	f.close()
	plt.hist(interArrivals)
	plt.show()
		
		
def jobGroupByInputPath(workload):
	# dictionary: key -- input path, value -- [numJobs, [jobIndices], [jobArrivalTimes], maxInputSize, time difference between the 1st and last jobs using the same input path]
	jobsGrouped = {}
	totalSizeByPath = 0
	totalSizeByJob = 0
	count = 0
	with open(workload, 'r') as tsvfile:
		tsvreader = csv.reader(tsvfile, delimiter="\t")
		for line in tsvreader:
			count += 1
			totalSizeByJob += int(line[3])
			if line[6] in jobsGrouped:
				jobsGrouped[line[6]][0] += 1
				jobsGrouped[line[6]][1].append(line[0])
				jobsGrouped[line[6]][2].append(int(line[1]))
				if int(line[3]) > jobsGrouped[line[6]][3]:
					jobsGrouped[line[6]][3] = int(line[3])
			else:
				totalSizeByPath += int(line[3])
				jobsGrouped[line[6]] = [1, [line[0]], [int(line[1])], int(line[3])]
	maxTimeDiff = 0
	for key, value in jobsGrouped.iteritems():
		timeDiff = max(value[2]) - min(value[2])
		if timeDiff > maxTimeDiff:
			maxTimeDiff = timeDiff
		value.append(timeDiff)	
		jobsGrouped[key] = value
	print 'maxTimeDiff = ' + str(maxTimeDiff)
	print 'totalSizeByJob: ' + str(totalSizeByJob/(1024*1024*1024)) + ' GB'
	print 'totalSizeByPath: ' + str(totalSizeByPath/(1024*1024*1024)) + ' GB'
	print 'total number of jobs is ' + str(count)
	print 'total number of paths is ' + str(len(jobsGrouped))
	pickle.dump(jobsGrouped, open('jobGroupByInputPath.txt', 'wb'))
	
	
def SHJobGroupByInputPath(workload):
	# dictionary: key -- input path, value -- [numJobs, [jobIndices], [jobArrivalTimes], maxInputSize, time difference between the 1st and last jobs using the same input path]
	SHJobsGrouped = {}
	totalSizeByPath = 0
	totalSizeByJob = 0
	count = 0
	tmp = 0
	with open(workload, 'r') as tsvfile:
		tsvreader = csv.reader(tsvfile, delimiter="\t")
		for line in tsvreader:
# 			if (int(line[4]) > int(line[3]) and int(line[4]) > 1024*1024*1024) or int(line[4]) > 10*1024*1024*1024:
			if int(line[4]) > ceil(int(line[3])*3/(4.0*1024**4))*1024**3 and int(line[4]) > 4.0*1024**4/3 \
			or int(line[3]) > 1024**3 and int(line[3]) <= 10*1024**3 and int(line[4]) > int(line[3])/2 \
			or int(line[3]) > 10*1024**3 and int(line[3]) <= 4.0*1024**4/3 and int(line[4]) > 3*1024**3 and int(line[4]) > int(line[3])/3 :
# 			if (int(line[4]) > int(line[3]) and int(line[4]) > 1024*1024*1024):
# 			if int(line[4]) > int(line[3]):
# 				print line[0] + ': input size = ' + str(int(line[3])/1024**3) + ', shuffle size = ' + str(int(line[4])/1024**3)
# 				print line
				totalSizeByJob += int(line[3])
				count += 1
				if line[6] in SHJobsGrouped:
					SHJobsGrouped[line[6]][0] += 1
					SHJobsGrouped[line[6]][1].append(line[0])
					SHJobsGrouped[line[6]][2].append(int(line[1]))
					if int(line[3]) > SHJobsGrouped[line[6]][3]:
						SHJobsGrouped[line[6]][3] = int(line[3])
					if int(line[4]) > SHJobsGrouped[line[6]][4]:
						SHJobsGrouped[line[6]][4] = int(line[4])
				else:
					totalSizeByPath += int(line[3])
					SHJobsGrouped[line[6]] = [1, [line[0]], [int(line[1])], int(line[3]), int(line[4])]
	maxTimeDiff = 0
	lastTime = 0
	for key, value in SHJobsGrouped.iteritems():
		timeDiff = max(value[2]) - min(value[2])
		if timeDiff > maxTimeDiff:
			maxTimeDiff = timeDiff
		value.append(timeDiff)
		timeDiffByPath = min(value[2]) - lastTime	
		value.append(timeDiffByPath)
		lastTime = max(value[2])
		SHJobsGrouped[key] = value
		if value[0] > 1:
			inputSize = value[3]
			shuffleSize = value[4]
			if shuffleSize > ceil(inputSize*3/(4.0*1024**4))*1024**3 and shuffleSize > 4.0*1024**4/3 \
			or inputSize > 1024**3 and inputSize <= 10*1024**3 and shuffleSize > inputSize/2 \
			or inputSize > 10*1024**3 and inputSize <= 4.0*1024**4/3 and shuffleSize > 3*1024**3 and shuffleSize > inputSize/3 :
				tmp += 1
				print key, value
	print tmp
	print '\nShuffle-heavy jobs:'
	print 'maxTimeDiff = ' + str(maxTimeDiff)
	print 'totalSizeByJob: ' + str(totalSizeByJob/(1024*1024*1024)) + ' GB'
	print 'totalSizeByPath: ' + str(totalSizeByPath/(1024*1024*1024)) + ' GB'
	print 'total number of jobs is ' + str(count)
	print 'total number of paths is ' + str(len(SHJobsGrouped))
	pickle.dump(SHJobsGrouped, open('SHJobGroupByInputPath.txt', 'wb'))
		
# inputShuffleSizes(workload)
# interArrivailTime(workload)

jobGroupByInputPath(workload)
SHJobGroupByInputPath(workload)













