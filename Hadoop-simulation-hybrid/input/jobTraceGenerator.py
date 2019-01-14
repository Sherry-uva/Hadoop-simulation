import sys, csv, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-hybrid')
from math import ceil
from random import expovariate, random, shuffle
from math import ceil

import preProcessPath
from HDFS import datasetPlaceWithPath
from AM import resourceRequestGenerator


# workload = "/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/FB-2010_samples_24_times_1hr_withInputPaths_0.tsv"

arrivalRate = float(sys.argv[2])
numTotalSHJs = int(sys.argv[1])
numTraces = int(sys.argv[3])

# traceDuration = float(sys.argv[1])
# trace = 'duration' + str(int(traceDuration)) + 'sec_SHJRatio' + str(int(SHJRatio)) + '%_arrivalRate' + str(arrivalRate)


# with open(workload, 'r') as tsvfile, open('input/SHJs-FB.csv', 'w') as output1, open('input/RJs-FB.csv', 'w') as output2:
# 	tsvreader = csv.reader(tsvfile, delimiter="\t")
# 	writer1 = csv.writer(output1)
# 	writer2 = csv.writer(output2)
# 	for job in tsvreader:
# 		if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000:
# 			writer1.writerow((job[0:7]))
# 		else:
# 			writer2.writerow((job[0:7]))
			
# with open('input/SHJs.csv', 'r') as csvfile1, open('input/RJs.csv', 'r') as csvfile2, open(workLoad, 'w') as jobMix:
# 	readerSHJ = csv.reader(csvfile1)
# 	readerRJ = csv.reader(csvfile2)
# 	writer = csv.writer(jobMix)
# 	numSHJs = 0
# 	numJobs = 0
# 	time = 0
# 	i = 0
# 	while time < traceDuration:
# 		interArriTime = int(ceil(expovariate(arrivalRate)))
# 		time += interArriTime
# 		numJobs += 1
# 		jobType = random()
# 		if jobType < SHJRatio/100:
# 			numSHJs += 1
# 			job = next(readerSHJ)
# 			job.append(job[0])
# 			job[0] = 'job' + str(i)
# 			job[1] = 'SHJ'
# 		else:
# 			job = next(readerRJ)
# 			job.append(job[0])
# 			job[0] = 'job' + str(i)
# 			job[1] = 'RJ'
# 		job[2] = interArriTime
# 		writer.writerow((job))
# 		i +=1
# print 'Total number of jobs in the trace is ' + str(numJobs)
# print 'number of SHJs: ' + str(numSHJs)

# numJobs = 0
# while numJobs < int(numTotalSHJs/(SHJRatio/100))-1 or numJobs > int(numTotalSHJs/(SHJRatio/100))+2:
# 	with open('input/SHJs-FB.csv', 'r') as csvfile1, open('input/RJs-larger.csv', 'r') as csvfile2, open(workLoad, 'w') as jobMix:
# 		readerSHJ = csv.reader(csvfile1)
# 		readerRJ = csv.reader(csvfile2)
# 		writer = csv.writer(jobMix)
# 		numSHJs = 0
# 		numJobs = 0
# 		time = 0
# 		i = 0
# 		while numSHJs < numTotalSHJs:
# 			interArriTime = int(ceil(expovariate(arrivalRate)))
# 			time += interArriTime
# 			numJobs += 1
# 			jobType = random()
# 			if jobType < SHJRatio/100:
# 				numSHJs += 1
# 				job = next(readerSHJ)
# 				while int(job[3]) > 8*10**11:
# 					job = next(readerSHJ)
# 				job[0] = 'job' + str(i)
# 				job[1] = time
# 				job[5] = 'SHJ'
# 			else:
# 				job = next(readerRJ)
# 				job[0] = 'job' + str(i)
# 				job[1] = time
# 				job[5] = 'RJ'
# 			job.append(int(ceil(int(job[3])/(1024.0*1024*128))))
# 			job.append(int(ceil(float(job[7])/8)))
# 			job[2] = interArriTime
# 			writer.writerow((job))
# 			i +=1
# 	print 'Total number of jobs in the trace is ' + str(numJobs)
# 	print 'number of SHJs: ' + str(numSHJs)

SHJs = []
RJs = []
with open('input/SHJs-FB.csv', 'r') as csvfile1, open('input/RJs.csv', 'r') as csvfile2:
	readerSHJ = csv.reader(csvfile1)
	readerRJ = csv.reader(csvfile2)
	for job in readerSHJ:
		SHJs.append(job)
	for job in readerRJ:
		RJs.append(job)

SHJRatios = [5]
for SHJRatio in SHJRatios:
	trace = 'numSHJ' + str(numTotalSHJs) + '_SHJRatio' + str(int(SHJRatio)) + '%_arrivalRate' + str(arrivalRate)		
	for index in range(numTraces):
		workLoad = '/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/' + trace + '_' + str(index) + '.csv'
		SHJIndices = range(len(SHJs))
		shuffle(SHJIndices)
		RJIndices = range(len(RJs))
		shuffle(RJIndices)
		numJobs = 0
		while numJobs < int(numTotalSHJs/(SHJRatio/100.0))-1 or numJobs > int(numTotalSHJs/(SHJRatio/100.0))+2:	
			numSHJs = 0
			numRJs = 0
			numJobs = 0
			time = 0
			i = 0
			with open(workLoad, 'w') as jobMix:
				writer = csv.writer(jobMix)
				while numSHJs < numTotalSHJs:
					interArriTime = int(ceil(expovariate(arrivalRate)))
					time += interArriTime
					numJobs += 1
					jobType = random()
					if jobType < SHJRatio/100.0:
						job = SHJs[SHJIndices[numSHJs]][:]
						numSHJs += 1
						job[0] = 'job' + str(i)
						job[1] = time
						job[5] = 'SHJ'
					else:
						job = RJs[RJIndices[numRJs]][:]
						numRJs += 1
						job[0] = 'job' + str(i)
						job[1] = time
						job[5] = 'RJ'
					job.append(int(ceil(int(job[3])/(1024.0*1024*128))))
					job.append(int(ceil(float(job[7])/8)))
					job[2] = interArriTime
					writer.writerow((job))
					i +=1
		print 'Total number of jobs in trace ' + trace + ' is ' + str(numJobs)
		print 'number of SHJs: ' + str(numSHJs)

# with open(workLoad, 'r') as csvfile, open('input/largeRJ_'+trace+'.txt', 'w') as output:
# 	reader = csv.reader(csvfile)
# 	count = 0
# 	maxInput = 0
# 	maxInputJob = []
# 	for job in reader:
# 		if int(job[4]) < 2*1024**3 and int(job[3]) > 100*1024**3:
# 			count += 1
# 			if int(job[3]) > 1024**4:
# 				print 'super large RJ'
# 				output.write("%s\n" % job[0])
# # 				output.write("%s, %s\n" % (job[0], job[6]))
# 			print job
# 			if int(job[3]) > maxInput:
# 				maxInput = int(job[3])
# 				maxInputJob = job
# 	print 'Total number of regular job with input larger than 100 GB is ' + str(count)
# 	print maxInputJob
# 
# 
# with open(workLoad, 'r') as csvfile, open('input/largeSHJ_'+trace+'.txt', 'w') as output:
# 	reader = csv.reader(csvfile)
# 	count = 0
# 	for job in reader:
# 		if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000 and int(job[3]) > 1024**4:
# 			count += 1
# 			print 'super large SHJ'
# 			output.write("%s\n" % job[0])
# 			print job
# print count	
# 
# 
# preProcessPath.processPath(workLoad, trace)	
# 
# datasetPlaceWithPath.populateHDFS(workLoad, trace)
# 
# resourceRequestGenerator.generator(workLoad, trace)













